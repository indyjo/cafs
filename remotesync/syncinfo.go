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
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/chunking"
	"github.com/indyjo/cafs/remotesync/shuffle"
)

// Struct SyncInfo contains information which two CAFS instances have to agree on before
// transmitting a file.
type SyncInfo struct {
	Chunks []chunkInfo         // hashes and sizes of chunks
	Perm   shuffle.Permutation // the permutation of chunks to use when transferring
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

func (s *SyncInfo) addChunk(key cafs.SKey, size int64) {
	s.Chunks = append(s.Chunks, chunkInfo{key, intsize(size)})
}

func intsize(size int64) int {
	if size < 0 || size > chunking.MaxChunkSize {
		panic("invalid chunk total")
	}
	return int(size)
}
