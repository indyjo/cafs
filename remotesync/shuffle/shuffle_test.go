package shuffle

import (
	"math/rand"
	"testing"
)

func TestShuffle(t *testing.T) {
	perm := Permutation{4, 6, 3, 1, 5, 2, 0}

	original := "0123456789abcde"
	shuffled := shuffleString(t, original, NewStreamShuffler(perm, nil))
	unshuffled := shuffleString(t, shuffled, NewInverseStreamShuffler(perm, nil))

	if original != unshuffled {
		t.Fatalf("Shuffling and unshuffling returned %#v. Expected: %#v", unshuffled, original)
	}
}

func shuffleString(t *testing.T, in string, s StreamShuffler) (out string) {
	t.Logf("Original: %v", in)
	f := func(v interface{}) error {
		out = out + string(v.(rune))
		return nil
	}
	s = s.WithFunc(f)
	for _, c := range in {
		s.Put(c)
	}
	s.End()
	t.Logf("Shuffled: %v", out)
	return
}

func TestTransmission(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	data := make([]int, 2048)
	for idx := range data {
		data[idx] = r.Int()
	}
	received := make(map[int]bool)

	type transmisson struct {
		sent     int
		shuffler StreamShuffler
		buf      chan int
	}
	transmissions := make([]transmission, 4)
	for idx := range transmissions {
		transmissions[idx].shuffler = NewStreamShuffler(Random(128, r), func(v interface{}) error {
			if !received[v.(int)] {
				received[v.(int)] = true
				transmissions[idx].sent++
			}
		})
	}

}
