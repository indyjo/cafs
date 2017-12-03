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

// Package shuffle implements an efficient algorithm for performing a
// cyclic permutation on a possibly infinite stream of data elements.
package shuffle

import "math/rand"

// Type Permutation contains a permutation of integer numbers 0..k-1,
// where k is the length of the permutation cycle.
type Permutation []int

// Type Shuffler implements a buffer for permuting a stream of
// data elements.
//
// Data elements are put into a Shuffler and retrieved from it
// in a different order. Internally using a buffer of size k, each
// data element is retrieved from the buffer up to k-1 steps after
// it is put in. A stream of data elemnts shuffled this way is
// reversible to its original order.
type Shuffler struct {
	perm   Permutation
	buffer []interface{}
	idx    int
}

// Interface StreamShuffler is common for shufflers and unshufflers working on a
// stream with a well-defined beginning and end.
type StreamShuffler interface {
	// Puts one data element into the StreamShuffler. Calls the ConsumeFunc zero or one times.
	Put(interface{}) error
	// Feeds remaining data from the buffer into the ConsumeFunc, calling it 0 to k times.
	End() error
	// Returns a shallow copy of this StreamShuffler with a different ConsumeFunc.
	WithFunc(consume ConsumeFunc) StreamShuffler
}

// Type ConsumeFunc defines a function that accepts one parameter of
// arbitrary type and returns an error.
type ConsumeFunc func(interface{}) error

// Type streamShuffler uses a Shuffler to permute a sequence of arbitrary length.
type streamShuffler struct {
	consume ConsumeFunc
	forward *Shuffler
}

// Type streamUnshuffler uses a Shuffler to restore the order of elements in a
// sequence of arbitrary length that was created using StreamShuffler.
type streamUnshuffler struct {
	consume ConsumeFunc
	forward *Shuffler
	inverse *Shuffler
}

// Creates a random permutation of given length.
func Random(size int, r *rand.Rand) Permutation {
	return r.Perm(size)
}

// Given a permutation p, creates a complimentary permutation p'
// such that using the output of a Shuffler based on p as the input
// of a Shuffler based on p' restores the original stream order
// delayed by p.Size() steps.
func (p Permutation) Inverse() Permutation {
	inv := make(Permutation, len(p))
	for i, j := range p {
		inv[j] = (len(p) - 1 + i) % len(p)
	}
	return inv
}

// Creates a new Shuffler based on permutation p.
func NewShuffler(p Permutation) *Shuffler {
	result := new(Shuffler)
	result.perm = p
	result.buffer = make([]interface{}, len(p))
	return result
}

// Inputs a data element v into the shuffler and simultaneously
// retrieves another (or, every k invocations, the same) data element.
// May return nil while the buffer hasn't been completely filled.
func (s *Shuffler) Put(v interface{}) interface{} {
	i := s.idx
	s.idx++
	if s.idx == len(s.buffer) {
		s.idx = 0
	}
	s.buffer[s.perm[i]] = v
	return s.buffer[i]
}

// Returns a complimentary shuffler that reverses the permutation (except
// for a delay of k-1 steps).
func (s *Shuffler) Inverse() *Shuffler {
	return NewShuffler(s.perm.Inverse())
}

// Returns the length k of the permutation buffer used by the shuffler.
func (s *Shuffler) Length() int {
	return len(s.buffer)
}

// Creates a StreamShuffler applying a permutation to a stream.
func NewStreamShuffler(p Permutation, consume ConsumeFunc) StreamShuffler {
	return &streamShuffler{
		consume: consume,
		forward: NewShuffler(p),
	}
}

func (e *streamShuffler) Put(v interface{}) error {
	if v == nil {
		panic("v may not be nil")
	}
	if r := e.forward.Put(v); r != nil {
		return e.consume(r)
	}
	return nil
}

func (e *streamShuffler) End() error {
	for i := 0; i < e.forward.Length(); i++ {
		if r := e.forward.Put(nil); r != nil {
			if err := e.consume(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *streamShuffler) WithFunc(consume ConsumeFunc) StreamShuffler {
	var s streamShuffler
	s = *e
	s.consume = consume
	return &s
}

// Creates a StreamShuffler applying the inverse permutation and thereby restoring
// the original stream order.
func NewInverseStreamShuffler(p Permutation, consume ConsumeFunc) StreamShuffler {
	return &streamUnshuffler{
		consume: consume,
		forward: NewShuffler(p),
		inverse: NewShuffler(p.Inverse()),
	}
}

var something interface{} = struct{}{}

func (e *streamUnshuffler) Put(v interface{}) error {
	for e.forward.Put(something) == nil {
		e.inverse.Put(nil)
	}
	if r := e.inverse.Put(v); r != nil {
		if err := e.consume(r); err != nil {
			return err
		}
	}
	return nil
}

func (e *streamUnshuffler) End() error {
	for i := 0; i < e.inverse.Length(); i++ {
		if r := e.inverse.Put(nil); r != nil {
			if err := e.consume(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *streamUnshuffler) WithFunc(consume ConsumeFunc) StreamShuffler {
	var s streamUnshuffler
	s = *e
	s.consume = consume
	return &s
}
