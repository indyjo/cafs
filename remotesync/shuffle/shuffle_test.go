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

// Test that doing multiple simultaneous transmissions via a buffered connection to a simultated
// caching receiver actually reduces the amount of data per transmission to the expected degree.
func TestTransmission(t *testing.T) {
	const NTRANSMISSIONS = 3
	const BUFFER_SIZE = 1000
	const PERMUTATION_SIZE = 8000

	// Simulate a cache of already received data. The value expresses at which time step the data is received.
	// That's how we can simulate buffering behavior.
	received := make(map[int]int)
	// Counter of data that was actually sent
	sent := 0

	// Time step.
	time := 0

	// Create a number of shufflers that simulate parallel transmissions.
	transmissions := make([]StreamShuffler, NTRANSMISSIONS)

	r := rand.New(rand.NewSource(0))
	for i := range transmissions {
		transmissions[i] = NewStreamShuffler(Random(PERMUTATION_SIZE, r), func(v interface{}) error {
			if v.(int) >= 0 {
				// Test if data has arrived at cache yet. If not, put it in and trigger retransmission
				if t_received, ok := received[v.(int)]; !ok || time < t_received {
					if !ok {
						received[v.(int)] = time + BUFFER_SIZE
					}
					sent++
				}
			}
			return nil
		})
	}

	for i := 0; i < PERMUTATION_SIZE; i++ {
		for _, transmission := range transmissions {
			if err := transmission.Put(i); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		time++
	}

	// Purposefully don't call End() on transmissions because that would not simulate parallel behavior.
	// Instead, flood the shufflers with a dummy value in parallel. By putting PERMUTATION_SIZE
	// dummy values per transmission, we can guarantee the buffer was flushed.
	for i := 0; i < PERMUTATION_SIZE; i++ {
		for _, transmission := range transmissions {
			if err := transmission.Put(-1); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		time++
	}

	transmission_avg := float64(sent) / float64(PERMUTATION_SIZE)
	t.Logf("Transmissions/data: % 5.2f", transmission_avg)

	// This model is probably wrong because it assumes stochastic independence of data chunk transmissions
	t.Logf("Expected:           % 5.2f", 1+float64(NTRANSMISSIONS-1)*float64(BUFFER_SIZE)/float64(PERMUTATION_SIZE))
	// TODO: Add actual test here
}
