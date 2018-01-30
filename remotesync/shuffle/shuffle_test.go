package shuffle

import (
	"math/rand"
	"testing"
)

func TestShuffler(t *testing.T) {
	rgen := rand.New(rand.NewSource(1))
	for _, permSize := range []int{1, 2, 3, 4, 5, 7, 10, 31, 57, 127, 512} {
		perm := Random(permSize, rgen)
		inv := perm.Inverse()
		for _, dataSize := range []int{1, 2, 3, 4, 5, 7, 10, 31, 57, 127, 512, 1024} {
			delay := len(perm) - 1

			for repeat := 0; repeat < 100; repeat++ {
				data := Random(dataSize, rgen)

				forward := NewShuffler(perm)
				inverse := NewShuffler(inv)

				for i := 0; i < len(data)+delay; i++ {
					var v interface{}
					if i < len(data) {
						v = data[i]
					} else {
						v = nil
					}
					w := inverse.Put(forward.Put(v))
					if i < delay {
						continue
					}
					if w.(int) != data[i-delay] {
						t.Errorf("When testing with permutations of size %v", permSize)
						t.Errorf("  and data of size %v,", dataSize)
						t.Fatalf("  a mismatch was detected on %vth iteration: w:%v != data[%v]:%v", i, w, i-delay, data[i-delay])
					}
				}
			}
		}
	}
}

func TestStreamShuffler(t *testing.T) {
	permutations := []Permutation{
		{0},
		{0, 1},
		{1, 0},
		{3, 4, 2, 1, 0},
		{4, 6, 3, 1, 5, 2, 0},
		{6, 7, 5, 3, 2, 1, 4, 0},
		{
			24, 40, 90, 31, 11, 6, 26, 54, 76, 43, 79, 92, 7, 49, 17, 32, 80,
			95, 15, 86, 20, 48, 94, 5, 27, 50, 65, 58, 38, 33, 60, 87, 36, 59,
			85, 55, 23, 72, 47, 53, 39, 71, 96, 74, 82, 83, 28, 97, 62, 3, 45, 21,
			2, 44, 70, 1, 25, 4, 68, 10, 19, 67, 77, 81, 51, 61, 35, 91, 84, 57,
			16, 64, 78, 73, 93, 34, 29, 8, 30, 9, 66, 89, 52, 22, 18, 56, 13, 46,
			69, 75, 88, 41, 42, 63, 12, 37, 14, 0,
		},
	}
	strings := []string{
		"",
		"x",
		"xy",
		"xyz",
		"0123456789abcde",
		"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
	}
	for _, perm := range permutations {
		for _, str := range strings {
			testWith(t, perm, str)
		}
	}
}

func testWith(t *testing.T, perm Permutation, original string) {
	shuffled := shuffleString(t, original, NewStreamShuffler(perm, '_', nil))
	unshuffled := shuffleString(t, shuffled, NewInverseStreamShuffler(perm, '_', nil))

	if original != unshuffled {
		t.Errorf("When testing with %#v (inverse: %#v):", perm, perm.Inverse())
		t.Fatalf("  Shuffling and unshuffling returned %#v. Expected: %#v. Shuffled: %#v",
			unshuffled, original, shuffled)
	}
}

func shuffleString(t *testing.T, in string, s StreamShuffler) (out string) {
	f := func(v interface{}) error {
		out = out + string(v.(rune))
		return nil
	}
	s = s.WithFunc(f)
	for _, c := range in {
		s.Put(c)
	}
	s.End()
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
		transmissions[i] = NewStreamShuffler(Random(PERMUTATION_SIZE, r), -1, func(v interface{}) error {
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
