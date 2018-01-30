//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2018 Jonas Eschenburg <jonas@bitwrk.net>
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
)

var ErrDisposed = errors.New("Disposed")
var ErrUnexpectedChunk = errors.New("Unexpected chunk")

// Interface FlushWriter acts like an io.Writer with an additional Flush method.
type FlushWriter interface {
	io.Writer
	Flush()
}

// Used by receiver to memorize information about a chunk in the window between
// putting it into the wishlist and receiving the actual chunk data.
type chunk struct {
	key       cafs.SKey // The hash key of the chunk
	file      cafs.File // A File if the chunk existed already, nil otherwise
	length    int       // The length of the chunk
	requested bool      // Whether the chunk was requested from the sender
}

// Type Builder contains state needed for the duration of a file transmission.
type Builder struct {
	done    chan struct{}
	storage cafs.FileStorage
	chunks  chan chunk
	info    string
	perm    shuffle.Permutation
}

// Returns a new receiver for reconstructing a file. Must eventually be disposed.
// The builder can then proceed reading a byte sequence encoded with EncodeChunkHashes,
// and output a "wishlist" of chunks that are missing in the local storage
// for complete reconstruction of the file.
func NewBuilder(storage cafs.FileStorage, perm shuffle.Permutation, windowSize int, info string) *Builder {
	p := make(shuffle.Permutation, len(perm))
	copy(p, perm)
	return &Builder{
		done:    make(chan struct{}),
		storage: storage,
		chunks:  make(chan chunk, windowSize),
		info:    info,
		perm:    p,
	}
}

// Disposes the receiver. Must be called exactly once per receiver. May cause the goroutines running
// WriteWishList and ReconstructFileFromRequestedChunks to terminate with error ErrDisposed.
func (b *Builder) Dispose() {
	close(b.done)
drain:
	for {
		select {
		case chunk := <-b.chunks:
			if chunk.length == 0 {
				break drain
			} else if chunk.file != nil {
				chunk.file.Dispose()
			}
		}
	}
}

// Reads a byte sequence encoded with WriteChunkHashes and
// outputs a bit stream with '1' for each missing chunk, and
// '0' for each chunk that is already available or already requested.
func (b *Builder) WriteWishList(_r io.Reader, w FlushWriter) error {
	if LoggingEnabled {
		log.Printf("Receiver: Begin WriteWishList")
		defer log.Printf("Receiver: End WriteWishList")
	}

	defer close(b.chunks)

	// We need ReadByte
	r := bufio.NewReader(_r)

	requested := make(map[cafs.SKey]bool)
	idx := 0
	var lastPos int64

	// Utility closure for creating informative error messages
	statusError := func(msg string, err error) error {
		return fmt.Errorf("Error %v after successfully reading %v chunks hashes up to byte position %v: %v",
			msg, idx, lastPos, err)
	}

	bitWriter := newBitWriter(w)

	for {
		// Read a chunk hash and its length
		var key cafs.SKey
		if _, err := io.ReadFull(r, key[:]); err == io.EOF {
			break
		} else if err != nil {
			return statusError("reading chunk hash", err)
		}
		var length int64
		if l, err := readChunkLength(r); err != nil {
			return statusError("reading length of chunk", err)
		} else {
			length = l
		}

		lastPos += length
		chunk := chunk{
			key:    key,
			length: int(length),
		}

		if key == emptyKey || requested[key] {
			// This key was already requested. Also, the empty key is never requested.
			chunk.requested = false
		} else if file, err := b.storage.Get(&key); err != nil {
			// File was not found in storage -> request and remember
			chunk.requested = true
			requested[key] = true
		} else {
			// File was already in storage -> prevent it from being collected until it is needed
			chunk.file = file
			chunk.requested = false
			requested[key] = true
		}

		// Write chunk info into channel. This might block if channel buffer is full.
		// Only wait until disposed.
		select {
		case b.chunks <- chunk:
		case <-b.done:
			return ErrDisposed
		}

		if err := bitWriter.WriteBit(chunk.requested); err != nil {
			return err
		}

		idx++
	}
	return bitWriter.Flush()
}

var placeholder interface{} = struct{}{}

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

	errDone := errors.New("Done")

	unshuffler := shuffle.NewInverseStreamShuffler(b.perm, placeholder, func(v interface{}) error {
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
		// Check if the builder has been disposed
		select {
		case <-b.done:
			return ErrDisposed
		default:
		}

		// Read chunk info from the channel
		chunkInfo := <-b.chunks
		if chunkInfo.file != nil {
			defer chunkInfo.file.Dispose()
		}

		if chunkInfo.key == emptyKey {
			return unshuffler.Put(placeholder)
		}

		// Under the following circumstances, read chunk data from the stream.
		//  - chunk data was requested
		//  - the chunk info stream has ended (to check whether the chunk data stream also ends).
		// If there was a real error, abort.
		if chunkInfo.requested || chunkInfo.key == zeroKey {
			chunkFile, err := readChunk(b.storage, r, fmt.Sprintf("%v #%d", b.info, idx))
			if chunkFile != nil {
				defer chunkFile.Dispose()
			}
			if err == io.EOF && chunkInfo.key == zeroKey {
				return errDone
			} else if err == io.EOF {
				return io.ErrUnexpectedEOF
			} else if err != nil {
				return err
			} else if chunkInfo.key == zeroKey {
				return fmt.Errorf("Unsolicited chunk data")
			} else if chunkFile.Key() != chunkInfo.key {
				return ErrUnexpectedChunk
			} else if chunkFile.Size() != int64(chunkInfo.length) {
				return ErrUnexpectedChunk
			}
		}

		// Retrieve the chunk from CAFS (we can expect to find it)
		chunk, _ := b.storage.Get(&chunkInfo.key)
		// ... and dispatch it to the unshuffler, where it will be buffered for a while.
		// Disposing is done by the unshuffler's ConsumeFunc.
		if LoggingEnabled {
			log.Printf("Receiver: unshuffler.Put(size:%v, %v)", chunk.Size(), chunk.Key())
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
		log.Printf("Receiver: appendChunk(size:%v, %v)", chunk.Size(), chunk.Key())
	}
	r := chunk.Open()
	defer r.Close()
	if _, err := io.Copy(temp, r); err != nil {
		return err
	}
	return nil
}
