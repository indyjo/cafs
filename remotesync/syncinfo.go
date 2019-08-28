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
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/chunking"
	"github.com/indyjo/cafs/remotesync/shuffle"
	"io"
)

// Struct SyncInfo contains information which two CAFS instances have to agree on before
// transmitting a file.
type SyncInfo struct {
	Chunks []chunkInfo         // hashes and sizes of chunks
	Perm   shuffle.Permutation // the permutation of chunks to use when transferring
}

// Func SetNoPermutation sets the prmutation to the trivial permutation (the one that doesn't permute).
func (s *SyncInfo) SetTrivialPermutation() {
	s.Perm = []int{0}
}

// Func SetPermutation sets the permutation to use when transferring chunks.
func (s *SyncInfo) SetPermutation(perm shuffle.Permutation) {
	s.Perm = append(s.Perm[:0], perm...)
}

// Func SetChunksFromFile prepares sync information for a CAFS file.
func (s *SyncInfo) SetChunksFromFile(file cafs.File) {
	if !file.IsChunked() {
		s.Chunks = append(s.Chunks[:0], chunkInfo{
			Key:  file.Key(),
			Size: intsize(file.Size()),
		})
		return
	}

	iter := file.Chunks()
	s.Chunks = s.Chunks[:0]
	for iter.Next() {
		s.addChunk(iter.Key(), iter.Size())
	}
	iter.Dispose()
}

// func ReadFromLegacyStream reads chunk hashes from a stream encoded in the format previously used. No permutation
// data is sent and it is expected that permutation remain the trivial permutation {0}.
func (s *SyncInfo) ReadFromLegacyStream(stream io.Reader) error {
	// We need ReadByte
	r := bufio.NewReader(stream)

	for {
		// Read a chunk hash and its size
		var key cafs.SKey
		if _, err := io.ReadFull(r, key[:]); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("error reading chunk hash: %v", err)
		}
		var size int64
		if l, err := readChunkLength(r); err != nil {
			return fmt.Errorf("error reading size of chunk: %v", err)
		} else {
			size = l
		}

		s.addChunk(key, size)
	}
	return nil
}

// Func WriteToLegacyStream writes chunk hashes to a stream encoded in the format previously used.
func (s *SyncInfo) WriteToLegacyStream(stream io.Writer) error {
	for _, ci := range s.Chunks {
		if _, err := stream.Write(ci.Key[:]); err != nil {
			return err
		}
		if err := writeVarint(stream, int64(ci.Size)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SyncInfo) addChunk(key cafs.SKey, size int64) {
	s.Chunks = append(s.Chunks, chunkInfo{key, intsize(size)})
}

func intsize(size int64) int {
	if size < 0 || size > chunking.MaxChunkSize {
		panic("invalid chunk total")
	}
	return int(size)
}
