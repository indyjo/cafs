//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2017 Jonas Eschenburg <jonas@bitwrk.net>
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

// Package remotesync implements a differential file synching mechanism based on the content-based chunking
// that is used by CAFS internally.
// Step 1: Sender lists hashes of chunks of file to transmit (32 byte + ~2.5 bytes for length per chunk)
// Step 2: Receiver lists missing chunks (one bit per chunk)
// Step 3: Sender sends content of missing chunks
package remotesync

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/chunking/adler32"
	"io"
	"log"
)

var LoggingEnabled = false

var ErrDisposed = errors.New("Disposed")
var ErrUnexpectedChunk = errors.New("Unexpected chunk")

var zeroKey cafs.SKey = cafs.SKey{}

type byteReader struct {
	r   io.Reader
	buf [1]byte
}

// By passing a callback function to some of the transmissions functions,
// the caller can subscribe to the current transmission status.
type TransferStatusCallback func(bytesToTransfer, bytesTransferred int64)

func (r byteReader) ReadByte() (byte, error) {
	_, err := io.ReadFull(r.r, r.buf[:])
	return r.buf[0], err
}

func readVarint(r io.Reader) (int64, error) {
	return binary.ReadVarint(byteReader{r: r})
}

func readChunkLength(r io.Reader) (int64, error) {
	if l, err := readVarint(r); err != nil {
		return 0, err
	} else if l < 0 || l > adler32.MAX_CHUNK {
		return 0, fmt.Errorf("Illegal chunk length: %v", l)
	} else {
		return l, nil
	}
}

func writeVarint(w io.Writer, value int64) error {
	var buf [binary.MaxVarintLen64]byte
	_, err := w.Write(buf[:binary.PutVarint(buf[:], value)])
	return err
}

// Writes a stream of chunk hash/length pairs into an io.Writer. Length is encoded
// as Varint.
func WriteChunkHashes(file cafs.File, w io.Writer) error {
	if LoggingEnabled {
		log.Printf("Sender: Begin WriteChunkHashes")
		defer log.Printf("Sender: End WriteChunkHashes")
	}
	chunks := file.Chunks()
	defer chunks.Dispose()
	for chunks.Next() {
		key := chunks.Key()
		if LoggingEnabled {
			log.Printf("Sender: Write %v", key)
		}
		if _, err := w.Write(key[:]); err != nil {
			return err
		}
		if err := writeVarint(w, chunks.Size()); err != nil {
			return err
		}
	}
	return nil
}

// Iterates over a wishlist (read from `r`) and calls `f` for each chunk of `file`, requested or not.
// If `f` returns an error, aborts the iteration and also returns the error.
func forEachChunk(file cafs.File, r io.ByteReader, f func(chunk cafs.File, requested bool) error) error {
	var b byte
	bit := 8
	iter := file.Chunks()
	defer iter.Dispose()

	for iter.Next() {
		if bit == 8 {
			var err error
			b, err = r.ReadByte()
			if err != nil {
				return fmt.Errorf("Wishlist too short: %v on chunk %v", err, iter.Key())
			}
			bit = 0
		}
		chunk := iter.File()
		err := f(chunk, 0 != (b&0x80))
		chunk.Dispose()
		if err != nil {
			return err
		}
		b <<= 1
		bit++
	}
	if _, err := r.ReadByte(); err != io.EOF {
		return errors.New("Wishlist too long")
	}
	return nil
}

// Writes a stream of chunk length / data pairs into an io.Writer, based on
// the chunks of a file and a matching list of requested chunks.
func WriteRequestedChunks(file cafs.File, r io.ByteReader, w io.Writer, cb TransferStatusCallback) error {
	if LoggingEnabled {
		log.Printf("Sender: Begin WriteRequestedChunks")
		defer log.Printf("Sender: End WriteRequestedChunks")
	}

	// Determine the number of bytes to transmit by starting at the maximum and subtracting chunk
	// size whenever we read a 0 (chunk not requested)
	bytesToTransfer := file.Size()
	if cb != nil {
		cb(bytesToTransfer, 0)
	}

	// Iterate requested chunks. Write the chunk's length (as varint) and the chunk data
	// into the output writer. Update the number of bytes transferred on the go.
	var bytesTransferred int64
	return forEachChunk(file, r, func(chunk cafs.File, requested bool) error {
		if requested {
			if err := writeVarint(w, chunk.Size()); err != nil {
				return err
			}
			r := chunk.Open()
			defer r.Close()
			if n, err := io.Copy(w, r); err != nil {
				return err
			} else {
				bytesTransferred += n
			}
		} else {
			bytesToTransfer -= chunk.Size()
		}
		if cb != nil {
			// Notify callback of status
			cb(bytesToTransfer, bytesTransferred)
		}
		return nil
	})
}

// Used by receiver to memorize information about a chunk in the window between
// putting it into the wishlist and receiving the actual chunk data.
type chunk struct {
	key       cafs.SKey // The hash key of the chunk
	file      cafs.File // A File if the chunk existed already, nil otherwise
	length    int       // The length of the chunk
	requested bool      // Whether the chunk was requested from the sender
}

type Builder struct {
	done     chan struct{}
	storage  cafs.FileStorage
	chunks   chan chunk
	disposed bool
	info     string
}

// Returns a new receiver for reconstructing a file. Must eventually be disposed.
// The builder can then proceed reading a byte sequence encoded with EncodeChunkHashes,
// and output a "wishlist" of chunks that are missing in the local storage
// for complete reconstruction of the file.
func NewBuilder(storage cafs.FileStorage, windowSize int, info string) *Builder {
	return &Builder{
		done:     make(chan struct{}),
		storage:  storage,
		chunks:   make(chan chunk, windowSize),
		disposed: false,
		info:     info,
	}
}

func (b *Builder) Dispose() {
	if !b.disposed {
		b.disposed = true
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
}

func (b *Builder) checkValid() {
	if b.disposed {
		panic(ErrDisposed)
	}
}

type bitWriter struct {
	w   io.Writer
	n   int
	buf [1]byte
}

func NewBitWriter(writer io.Writer) *bitWriter {
	return &bitWriter{w: writer}
}

func (w *bitWriter) WriteBit(b bool) (err error) {
	if b {
		w.buf[0] = (w.buf[0] << 1) | 1
	} else {
		w.buf[0] = (w.buf[0] << 1)
	}
	w.n++
	if w.n == 8 {
		_, err = w.w.Write(w.buf[:])
		w.n = 0
	}
	return
}

func (w *bitWriter) Flush() (err error) {
	for err == nil && w.n != 0 {
		err = w.WriteBit(false)
	}
	return
}

// Reads a byte sequence encoded with EncodeChunkHashes and
// outputs a bit stream with '1' for each missing chunk, and
// '0' for each chunk that is already available or already requested.
func (b *Builder) WriteWishList(r io.Reader, w io.Writer) error {
	b.checkValid()

	if LoggingEnabled {
		log.Printf("Receiver: Begin WriteWishList")
		defer log.Printf("Receiver: End WriteWishList")
	}

	defer close(b.chunks)

	requested := make(map[cafs.SKey]bool)
	idx := 0
	var lastPos int64

	// Utility closure for creating informative error messages
	statusError := func(msg string, err error) error {
		return fmt.Errorf("Error %v after successfully reading %v chunks hashes up to byte position %v: %v",
			msg, idx, lastPos, err)
	}

	bitWriter := NewBitWriter(w)

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

		if requested[key] {
			// This key was already requested
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

		b.chunks <- chunk

		if err := bitWriter.WriteBit(chunk.requested); err != nil {
			return err
		}

		idx++
	}
	return bitWriter.Flush()
}

// Reads a sequence of length-prefixed data chunks and tries to reconstruct a file from that
// information.
func (b *Builder) ReconstructFileFromRequestedChunks(r io.Reader) (cafs.File, error) {
	b.checkValid()

	if LoggingEnabled {
		log.Printf("Receiver: Begin ReconstructFileFromRequestedChunks")
		defer log.Printf("Receiver: End ReconstructFileFromRequestedChunks")
	}

	temp := b.storage.Create(b.info)
	defer temp.Dispose()

	errDone := errors.New("Done")

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

		// Retrieve the chunk from CAFS (we can expect to find it) and
		// verify that we got the correct chunk.
		chunk, _ := b.storage.Get(&chunkInfo.key)
		defer chunk.Dispose()

		// Write a chunk of the work file
		return appendChunk(temp, chunk)
	}

	for {
		if err := iteration(); err == errDone {
			break
		} else if err != nil {
			return nil, err
		}
		idx++
	}

	if err := temp.Close(); err != nil {
		return nil, err
	}

	return temp.File(), nil
}

func appendChunk(temp cafs.Temporary, chunk cafs.File) error {
	r := chunk.Open()
	defer r.Close()
	if _, err := io.Copy(temp, r); err != nil {
		return err
	}
	return nil
}

func readChunk(s cafs.FileStorage, r io.Reader, info string) (cafs.File, error) {
	var length int64
	if n, err := readVarint(r); err != nil {
		return nil, err
	} else {
		length = n
	}
	if length < 0 || length > adler32.MAX_CHUNK {
		return nil, fmt.Errorf("Invalid chunk length: %v", length)
	}
	tempChunk := s.Create(info)
	defer tempChunk.Dispose()
	if _, err := io.CopyN(tempChunk, r, length); err != nil {
		return nil, err
	}
	if err := tempChunk.Close(); err != nil {
		return nil, err
	}
	return tempChunk.File(), nil
}
