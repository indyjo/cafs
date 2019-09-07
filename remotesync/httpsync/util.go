//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2019 Jonas Eschenburg <jonas@bitwrk.net>
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.package main

package httpsync

import (
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/remotesync"
	"io"
	"sync"
	"time"
)

// Interface chunksSource specifies a factory for Chunks
type chunksSource interface {
	GetChunks() (remotesync.Chunks, error)
	Dispose()
}

// struct fileBasedChunksSource implements ChunksSource using a File.
type fileBasedChunksSource struct {
	m    sync.Mutex
	file cafs.File
}

func (f fileBasedChunksSource) GetChunks() (remotesync.Chunks, error) {
	f.m.Lock()
	file := f.file
	f.m.Unlock()
	if file == nil {
		return nil, remotesync.ErrDisposed
	}
	return remotesync.ChunksOfFile(file), nil
}

func (f fileBasedChunksSource) Dispose() {
	f.m.Lock()
	file := f.file
	f.file = nil
	f.m.Unlock()
	if file != nil {
		file.Dispose()
	}
}

// Struct syncInfoChunksSource implements ChunksSource using only a SyncInfo object.
// It requests chunks from a FileStore and waits until that chunk becomes available.
// There is no guarantee that chunks are kept or will actually become available at some
// time.
type syncInfoChunksSource struct {
	syncinfo *remotesync.SyncInfo
	storage  cafs.FileStorage
}

func (s syncInfoChunksSource) GetChunks() (remotesync.Chunks, error) {
	return &syncInfoChunks{
		chunks:  s.syncinfo.Chunks,
		storage: s.storage,
		done:    make(chan struct{}),
	}, nil
}

func (s syncInfoChunksSource) Dispose() {
}

// Struct syncInfoChunks implements the Chunks interface and does the actual waiting.
type syncInfoChunks struct {
	chunks  []remotesync.ChunkInfo
	storage cafs.FileStorage
	done    chan struct{}
}

func (s *syncInfoChunks) NextChunk() (cafs.File, error) {
	if len(s.chunks) == 0 {
		return nil, io.EOF
	}
	key := s.chunks[0].Key
	s.chunks = s.chunks[1:]
	ticker := time.NewTicker(100 * time.Millisecond)
	defer func() {
		ticker.Stop()
	}()
	for {
		if f, err := s.storage.Get(&key); err == nil {
			return f, nil
		} else if err != cafs.ErrNotFound {
			return nil, err
		}

		select {
		case <-s.done:
			return nil, remotesync.ErrDisposed
		case <-ticker.C:
			// next try
		}
	}
}

func (s *syncInfoChunks) Dispose() {
	close(s.done)
}

// An implementation of FlushWriter whose Flush() function is a nop.
type nopFlushWriter struct {
	w io.Writer
}

func (f nopFlushWriter) Write(p []byte) (n int, err error) {
	return f.w.Write(p)
}

func (f nopFlushWriter) Flush() {
}
