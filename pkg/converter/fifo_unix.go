//go:build !windows
// +build !windows

/*
 * Copyright (c) 2022. Nydus Developers. All rights reserved.
 * Copyright The containerd Authors.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package converter

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"

	"github.com/containerd/fifo"
)

// pathFifo is the non-blocking fifo of github.com/containerd/fifo with the
// handle of its non-Linux build: it opens the pipe by its path. The Linux
// handle opens it through /proc/self/fd, and a sandbox can have no /proc.
type pathFifo struct {
	path        string
	dev         uint64
	ino         uint64
	flag        int
	opened      chan struct{}
	closed      chan struct{}
	closing     chan struct{}
	err         error
	file        *os.File
	closingOnce sync.Once
	closedOnce  sync.Once
}

// openFifo makes the named pipe at path if it does not exist and returns
// before the other end opens it. flag is syscall.O_RDONLY or syscall.O_WRONLY.
func openFifo(ctx context.Context, path string, flag int, perm os.FileMode) (*pathFifo, error) {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := syscall.Mkfifo(path, uint32(perm&os.ModePerm)); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("error creating fifo %v: %w", path, err)
		}
	}

	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return nil, fmt.Errorf("failed to stat %v: %w", path, err)
	}

	f := &pathFifo{
		path:    path,
		dev:     uint64(stat.Dev), //nolint:unconvert
		ino:     uint64(stat.Ino), //nolint:unconvert
		flag:    flag,
		opened:  make(chan struct{}),
		closed:  make(chan struct{}),
		closing: make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-f.opened:
			default:
				f.Close()
			}
		case <-f.opened:
		case <-f.closed:
		}
	}()
	go func() {
		var file *os.File
		path, err := f.verifiedPath()
		if err == nil {
			file, err = os.OpenFile(path, flag, 0)
		}
		select {
		case <-f.closing:
			if err == nil {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				default:
					err = fmt.Errorf("fifo %v was closed before opening", f.path)
				}
				if file != nil {
					file.Close()
				}
			}
		default:
		}
		if err != nil {
			f.closedOnce.Do(func() {
				f.err = err
				close(f.closed)
			})
			return
		}
		f.file = file
		close(f.opened)
	}()
	return f, nil
}

func (f *pathFifo) verifiedPath() (string, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(f.path, &stat); err != nil {
		return "", fmt.Errorf("path %v could not be statted: %w", f.path, err)
	}
	if uint64(stat.Dev) != f.dev || uint64(stat.Ino) != f.ino { //nolint:unconvert
		return "", fmt.Errorf("failed to verify handle %v/%v %v/%v for %v", stat.Dev, f.dev, stat.Ino, f.ino, f.path)
	}
	return f.path, nil
}

func (f *pathFifo) Read(b []byte) (int, error) {
	if f.flag&syscall.O_WRONLY > 0 {
		return 0, fifo.ErrRdFrmWRONLY
	}
	select {
	case <-f.opened:
		return f.file.Read(b)
	default:
	}
	select {
	case <-f.opened:
		return f.file.Read(b)
	case <-f.closed:
		return 0, fifo.ErrReadClosed
	}
}

func (f *pathFifo) Write(b []byte) (int, error) {
	if f.flag&syscall.O_WRONLY == 0 {
		return 0, fifo.ErrWrToRDONLY
	}
	select {
	case <-f.opened:
		return f.file.Write(b)
	default:
	}
	select {
	case <-f.opened:
		return f.file.Write(b)
	case <-f.closed:
		return 0, fifo.ErrWriteClosed
	}
}

// Close closes the pipe. If the other end did not open the pipe, Close opens
// that end for a moment, which releases the open call of this end.
func (f *pathFifo) Close() (retErr error) {
	for {
		select {
		case <-f.closed:
			return
		default:
		}
		select {
		case <-f.opened:
			f.closedOnce.Do(func() {
				retErr = f.file.Close()
				f.err = retErr
				close(f.closed)
			})
			continue
		default:
		}
		f.closingOnce.Do(func() {
			close(f.closing)
		})
		reverseMode := syscall.O_WRONLY
		if f.flag&syscall.O_WRONLY > 0 {
			reverseMode = syscall.O_RDONLY
		}
		path, err := f.verifiedPath()
		select {
		case <-f.closed:
		default:
			if err != nil {
				// The path does not name the pipe now, thus the open call
				// of this end can stay blocked.
				f.closedOnce.Do(func() {
					f.err = err
					close(f.closed)
				})
				continue
			}
			other, err := os.OpenFile(path, reverseMode|syscall.O_NONBLOCK, 0)
			if err == nil {
				other.Close()
			}
			runtime.Gosched()
		}
	}
}
