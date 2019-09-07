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
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

package remotesync

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/remotesync/shuffle"
	"io"
	"log"
	"sync"
)

var ErrDisposed = errors.New("disposed")
var ErrUnexpectedChunk = errors.New("unexpected chunk")

// Used by receiver to memorize information about a chunk in the time window between
// putting it into the wishlist and receiving the actual chunk data.
type memo struct {
	ci        ChunkInfo // key and length
	file      cafs.File // A File if the chunk existed already, nil otherwise
	requested bool      // Whether the chunk was requested from the sender
}

// Type Builder contains state needed for the duration of a file transmission.
type Builder struct {
	done    chan struct{}
	storage cafs.FileStorage
	memos   chan memo
	info    string
	syncinf *SyncInfo

	mutex    sync.Mutex // Guards subsequent variables
	disposed bool       // Set in Dispose
	started  bool       // Set in WriteWishList. Signals that chunks channel will be used.
}

// Returns a new Builder for reconstructing a file. Must eventually be disposed.
// The builder can then proceed sending a "wishlist" of chunks that are missing
// in the local storage for complete reconstruction of the file.
func NewBuilder(storage cafs.FileStorage, syncinf *SyncInfo, windowSize int, info string) *Builder {
	return &Builder{
		done:    make(chan struct{}),
		storage: storage,
		memos:   make(chan memo, windowSize),
		info:    info,
		syncinf: syncinf,
	}
}

// Disposes the Builder. Must be called exactly once per Builder. May cause the goroutines running
// WriteWishList and ReconstructFileFromRequestedChunks to terminate with error ErrDisposed.
func (b *Builder) Dispose() {
	b.mutex.Lock()
	if b.disposed {
		panic("Builder must be disposed exactly once")
	}
	b.disposed = true
	started := b.started
	b.mutex.Unlock()

	close(b.done)

	if started {
		for chunk := range b.memos {
			if chunk.file != nil {
				chunk.file.Dispose()
			}
		}
	}
}

// Outputs a bit stream with '1' for each missing chunk, and
// '0' for each chunk that is already available or already requested.
func (b *Builder) WriteWishList(w FlushWriter) error {
	if LoggingEnabled {
		log.Printf("Receiver: Begin WriteWishList")
		defer log.Printf("Receiver: End WriteWishList")
	}

	if err := b.start(); err != nil {
		return err
	}

	defer close(b.memos)

	requested := make(map[cafs.SKey]bool)
	bitWriter := newBitWriter(w)

	consumeFunc := func(v interface{}) error {
		ci := v.(ChunkInfo)
		key := ci.Key

		mem := memo{
			ci: ci,
		}

		if key == emptyKey || requested[key] {
			// This key was already requested. Also, the empty key is never requested.
			mem.requested = false
		} else if file, err := b.storage.Get(&key); err != nil {
			// File was not found in storage -> request and remember
			mem.requested = true
			requested[key] = true
		} else {
			// File was already in storage -> prevent it from being collected until it is needed
			mem.file = file
			mem.requested = false
			requested[key] = true
		}

		// Write memo into channel. This might block if channel buffer is full.
		// Only wait until disposed.
		select {
		case b.memos <- mem:
			// Responsibility for disposing chunk.file is passed to the channel
		case <-b.done:
			if mem.file != nil {
				mem.file.Dispose()
			}
			return ErrDisposed
		}

		if err := bitWriter.WriteBit(mem.requested); err != nil {
			return err
		}

		return nil // success
	}

	// Create a shuffler using the above consumeFunc and push the SyncInfo's chunk infos through it.
	// For every ChunkInfo leaving the shuffler (in shuffled order), the consumeFunc
	// writes a bit into the wishlist.
	shuffler := shuffle.NewStreamShuffler(b.syncinf.Perm, emptyChunkInfo, consumeFunc)
	nChunks := len(b.syncinf.Chunks)
	for idx := 0; idx < nChunks; idx++ {
		if err := shuffler.Put(b.syncinf.Chunks[idx]); err != nil {
			return fmt.Errorf("error from shuffler.Put: %v", err)
		}
	}
	if err := shuffler.End(); err != nil {
		return fmt.Errorf("error from shuffler.End: %v", err)
	}
	return bitWriter.Flush()
}

// Function start is called by WriteWishList to mark the Builder as started.
// This has consequences for the Dispose method.
func (b *Builder) start() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.disposed {
		return ErrDisposed
	}
	if b.started {
		panic("WriteWishList called twice")
	}
	b.started = true
	return nil
}

var placeholder interface{} = struct{}{}
var zeroMemo = memo{}

// Reads a sequence of length-prefixed data chunks and tries to reconstruct a file from that
// information.
func (b *Builder) ReconstructFileFromRequestedChunks(_r io.Reader) (cafs.File, error) {
	if LoggingEnabled {
		log.Printf("Receiver: Begin ReconstructFileFromRequestedChunks")
		defer log.Printf("Receiver: End ReconstructFileFromRequestedChunks")
	}

	temp := b.storage.Create(b.info)
	defer temp.Dispose()

	r := bufio.NewReader(_r)

	errDone := errors.New("done")

	unshuffler := shuffle.NewInverseStreamShuffler(b.syncinf.Perm, placeholder, func(v interface{}) error {
		chunk := v.(cafs.File)
		// Write a chunk of the work file
		err := appendChunk(temp, chunk)
		chunk.Dispose()
		return err
	})

	// Make sure all chunks in the unshuffler are disposed in the end
	defer unshuffler.WithFunc(func(v interface{}) error {
		v.(cafs.File).Dispose()
		return nil
	}).End()

	idx := 0
	iteration := func() error {
		var mem memo

		// Wait until either a chunk info can be read from the channel, or the builder
		// has been disposed.
		select {
		case <-b.done:
			return ErrDisposed
		case mem = <-b.memos:
			// successfully read, continue...
		}

		// It is our responsibility to dispose the file.
		if mem.file != nil {
			defer mem.file.Dispose()
		}

		if mem.ci == emptyChunkInfo {
			return unshuffler.Put(placeholder)
		}

		// Under the following circumstances, read chunk data from the stream.
		//  - chunk data was requested
		//  - the chunk memo stream has ended (to check whether the chunk data stream also ends).
		// If there was a real error, abort.
		if mem.requested || mem == zeroMemo {
			chunkFile, err := readChunk(b.storage, r, fmt.Sprintf("%v #%d", b.info, idx))
			if chunkFile != nil {
				defer chunkFile.Dispose()
			}
			if err == io.EOF && mem == zeroMemo {
				return errDone
			} else if err == io.EOF {
				return io.ErrUnexpectedEOF
			} else if err != nil {
				return err
			} else if mem == zeroMemo {
				return fmt.Errorf("unsolicited chunk data")
			} else if chunkFile.Key() != mem.ci.Key {
				return ErrUnexpectedChunk
			} else if chunkFile.Size() != int64(mem.ci.Size) {
				return ErrUnexpectedChunk
			}
		}

		// Retrieve the chunk from CAFS (we can expect to find it)
		chunk, _ := b.storage.Get(&mem.ci.Key)
		// ... and dispatch it to the unshuffler, where it will be buffered for a while.
		// Disposing is done by the unshuffler's ConsumeFunc.
		if LoggingEnabled {
			log.Printf("Receiver: unshuffler.Put(total:%v, %v)", chunk.Size(), chunk.Key())
		}
		return unshuffler.Put(chunk)
	}

	for {
		if err := iteration(); err == errDone {
			break
		} else if err != nil {
			return nil, err
		}
		idx++
	}

	if err := unshuffler.End(); err != nil {
		return nil, err
	}

	if err := temp.Close(); err != nil {
		return nil, err
	}

	return temp.File(), nil
}

// Function appendChunk appends data of `chunk` to `temp`.
func appendChunk(temp io.Writer, chunk cafs.File) error {
	if LoggingEnabled {
		log.Printf("Receiver: appendChunk(total:%v, %v)", chunk.Size(), chunk.Key())
	}
	r := chunk.Open()
	//noinspection GoUnhandledErrorResult
	defer r.Close()
	if _, err := io.Copy(temp, r); err != nil {
		return err
	}
	return nil
}
