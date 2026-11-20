//go:build !windows
// +build !windows

/*
 * Copyright (c) 2022. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package converter

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/fifo"
	"github.com/stretchr/testify/require"
)

func TestOpenFifoCopiesFromReaderToWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	reader, err := openFifo(context.Background(), path, syscall.O_RDONLY, 0640)
	require.NoError(t, err)
	writer, err := openFifo(context.Background(), path, syscall.O_WRONLY, 0640)
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.ModeNamedPipe|0640, info.Mode())

	go func() {
		_, _ = writer.Write([]byte("layer"))
		writer.Close()
	}()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "layer", string(data))
	require.NoError(t, reader.Close())

	_, err = reader.Write(nil)
	require.ErrorIs(t, err, fifo.ErrWrToRDONLY)
	_, err = writer.Read(nil)
	require.ErrorIs(t, err, fifo.ErrRdFrmWRONLY)
}

func TestOpenFifoCloseBeforeTheOtherEndOpens(t *testing.T) {
	for _, flag := range []int{syscall.O_RDONLY, syscall.O_WRONLY} {
		pipe, err := openFifo(context.Background(), filepath.Join(t.TempDir(), "pipe"), flag, 0640)
		require.NoError(t, err)
		require.NoError(t, pipe.Close())
		<-pipe.closed
		require.Nil(t, pipe.file)

		_, err = pipe.Read(make([]byte, 1))
		if flag == syscall.O_RDONLY {
			require.ErrorIs(t, err, fifo.ErrReadClosed)
		}
		_, err = pipe.Write([]byte{0})
		if flag == syscall.O_WRONLY {
			require.ErrorIs(t, err, fifo.ErrWriteClosed)
		}
	}
}

func TestOpenFifoClosesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := openFifo(ctx, filepath.Join(t.TempDir(), "pipe"), syscall.O_RDONLY, 0640)
	require.NoError(t, err)

	cancel()
	select {
	case <-pipe.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the pipe stayed open after the context cancel")
	}
	require.ErrorIs(t, pipe.err, context.Canceled)
}

func TestOpenFifoCloseAfterThePathIsGone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	pipe, err := openFifo(context.Background(), path, syscall.O_RDONLY, 0640)
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))

	require.NoError(t, pipe.Close())
	_, err = pipe.Read(make([]byte, 1))
	require.ErrorIs(t, err, fifo.ErrReadClosed)
}
