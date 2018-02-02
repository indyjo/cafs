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
	// Puts one data element into the StreamShuffler. Calls the ConsumeFunc exactly once.
	Put(interface{}) error
	// Feeds remaining data from the buffer into the ConsumeFunc, calling it k-1 times.
	End() error
	// Returns a shallow copy of this StreamShuffler with a different ConsumeFunc.
	WithFunc(consume ConsumeFunc) StreamShuffler
}

// Type ConsumeFunc defines a function that accepts one parameter of
// arbitrary type and returns an error.
type ConsumeFunc func(interface{}) error

// Functions of type applyFunc are used internally to apply a value returned from
// the shuffler to a ConsumeFunc. This is necessary because shuffling and unshuffling
// are not fully symmetric and require the substitution or removal of placeholder values.
type applyFunc func(ConsumeFunc, interface{}) error

// Type streamShuffler uses a Shuffler to permute a sequence of arbitrary length.
type streamShuffler struct {
	consume  ConsumeFunc
	shuffler *Shuffler
	apply    applyFunc
}

// Creates a random permutation of given length.
func Random(size int, r *rand.Rand) Permutation {
	return r.Perm(size)
}

// Given a permutation p, creates a complimentary permutation p'
// such that using the output of a Shuffler based on p as the input
// of a Shuffler based on p' restores the original stream order
// delayed by len(p) - 1 steps.
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

// Creates a StreamShuffler applying a permutation to a stream. Argument `placeholder`
// specifies a value that is inserted into the permuted stream in order to symbolize blank space.
func NewStreamShuffler(p Permutation, placeholder interface{}, consume ConsumeFunc) StreamShuffler {
	return &streamShuffler{
		consume:  consume,
		shuffler: NewShuffler(p),
		apply: func(f ConsumeFunc, v interface{}) error {
			if v == nil {
				v = placeholder
			}
			return f(v)
		},
	}
}

func (e *streamShuffler) Put(v interface{}) error {
	r := e.shuffler.Put(v)
	return e.apply(e.consume, r)
}

func (e *streamShuffler) End() error {
	for i := 0; i < e.shuffler.Length()-1; i++ {
		r := e.shuffler.Put(nil)
		if err := e.apply(e.consume, r); err != nil {
			return err
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
// the original stream order. Argument `placeholder` specifies blank space inserted
// into the stream by the original shuffler. Values equal to `placeholder` will not
// be forwarded to `consume`.
func NewInverseStreamShuffler(p Permutation, placeholder interface{}, consume ConsumeFunc) StreamShuffler {
	return &streamShuffler{
		consume:  consume,
		shuffler: NewShuffler(p.Inverse()),
		apply: func(f ConsumeFunc, v interface{}) error {
			if v == nil || v == placeholder {
				return nil
			}
			return f(v)
		},
	}
}
