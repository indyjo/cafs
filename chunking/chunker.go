//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2017  Jonas Eschenburg <jonas@bitwrk.net>
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

// Package chunking implements an algorithm for content-based chunking of arbitrary files.
package chunking

import "github.com/indyjo/cafs/chunking/adler32"

type Chunker interface {
	// Scans the byte sequence for chunk boundaries.
	// Returns the number of bytes from data that can be added to the current chunk.
	// A return value of len(data) means that no chunk boundary has been found in this block.
	Scan(data []byte) int
}

// Function New returns a new chunker.
func New() Chunker {
	return adler32.NewChunker()
}
