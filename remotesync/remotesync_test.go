package remotesync

import (
	"bufio"
	"fmt"
	"github.com/indyjo/cafs"
	. "github.com/indyjo/cafs/ram"
	"github.com/indyjo/cafs/remotesync/shuffle"
	"io"
	"math/rand"
	"testing"
)

// This is a regression test that deadlocks as long as indyjo/bitwrk#152 isn't solved.
// https://github.com/indyjo/bitwrk/issues/152
func TestDispose(t *testing.T) {
	store := NewRamStorage(256 * 1024)
	syncinfo := &SyncInfo{}
	syncinfo.SetPermutation(rand.Perm(10))
	builder := NewBuilder(store, syncinfo, 8, "Test file")
	// Dispose builder before call to WriteWishList
	builder.Dispose()
}

func TestRemoteSync(t *testing.T) {
	// Re-use stores to test for leaks on the fly
	storeA := NewRamStorage(8 * 1024 * 1024)
	storeB := NewRamStorage(8 * 1024 * 1024)
	// LoggingEnabled = true

	// Test for different amounts of overlapping data
	for _, p := range []float64{0, 0.01, 0.25, 0.5, 0.75, 0.99, 1} {
		// Test for different number of blocks, so that storeB will _almost_ be filled up.
		// We can't test up to 512 because we don't know how much overhead data was produced
		// by the chunking algorithm (yes, RAM storage counts that overhead!)
		for _, nBlocks := range []int{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 400} {
			sigma := 0.25
			if nBlocks > 256 {
				sigma = 0
			}
			// Test for different permutation sizes
			for _, permSize := range []int{1, 2, 3, 5, 10, 100, 1000} {
				perm := shuffle.Permutation(rand.Perm(permSize))
				func() {
					defer reportUsage(t, "B", storeB)
					defer reportUsage(t, "A", storeA)
					testWithParams(t, storeA, storeB, p, sigma, nBlocks, perm)
				}()
			}
		}
	}
}

func check(t *testing.T, msg string, err error) {
	if err != nil {
		t.Fatalf("Error %v: %v", msg, err)
	}
}

type flushWriter struct {
	w io.Writer
}

func (w flushWriter) Write(buf []byte) (int, error) {
	return w.w.Write(buf)
}

func (w flushWriter) Flush() {
}

func testWithParams(t *testing.T, storeA, storeB cafs.BoundedStorage, p, sigma float64, nBlocks int, perm shuffle.Permutation) {
	t.Logf("Testing with params: p=%f, nBlocks=%d, permSize=%d", p, nBlocks, len(perm))
	tempA := storeA.Create(fmt.Sprintf("Data A(%.2f,%d)", p, nBlocks))
	defer tempA.Dispose()
	tempB := storeB.Create(fmt.Sprintf("Data B(%.2f,%d)", p, nBlocks))
	defer tempB.Dispose()

	check(t, "creating similar data", createSimilarData(tempA, tempB, p, sigma, 8192, nBlocks))

	check(t, "closing tempA", tempA.Close())
	check(t, "closing tempB", tempB.Close())

	fileA := tempA.File()
	defer fileA.Dispose()

	syncinf := &SyncInfo{}
	syncinf.SetPermutation(perm)
	syncinf.SetChunksFromFile(fileA)
	builder := NewBuilder(storeB, syncinf, 8, fmt.Sprintf("Recovered A(%.2f,%d)", p, nBlocks))
	defer builder.Dispose()

	// task: transfer file A to storage B
	// Pipe 1 is used to transfer the wishlist bit-stream from the receiver to the sender
	pipeReader1, pipeWriter1 := io.Pipe()
	// Pipe 2 is used to transfer the actual requested chunk data to the receiver
	pipeReader2, pipeWriter2 := io.Pipe()

	go func() {
		if err := builder.WriteWishList(flushWriter{pipeWriter1}); err != nil {
			_ = pipeWriter1.CloseWithError(fmt.Errorf("Error generating wishlist: %v", err))
		} else {
			_ = pipeWriter1.Close()
		}
	}()

	go func() {
		chunks := ChunksOfFile(fileA)
		defer chunks.Dispose()
		if err := WriteChunkData(chunks, fileA.Size(), bufio.NewReader(pipeReader1), perm, pipeWriter2, nil); err != nil {
			_ = pipeWriter2.CloseWithError(fmt.Errorf("Error sending requested chunk data: %v", err))
		} else {
			_ = pipeWriter2.Close()
		}
	}()

	var fileB cafs.File
	if f, err := builder.ReconstructFileFromRequestedChunks(pipeReader2); err != nil {
		t.Fatalf("Error reconstructing: %v", err)
	} else {
		fileB = f
		defer f.Dispose()
	}

	_ = fileB
	assertEqual(t, fileA.Open(), fileB.Open())
}

func assertEqual(t *testing.T, a, b io.ReadCloser) {
	bufA := make([]byte, 1)
	bufB := make([]byte, 1)
	count := 0
	for {
		nA, errA := a.Read(bufA)
		nB, errB := b.Read(bufB)
		if nA != nB {
			t.Fatal("Chunks differ in total")
		}
		if errA != errB {
			t.Fatalf("Error a:%v b:%v", errA, errB)
		}
		if bufA[0] != bufB[0] {
			t.Fatalf("Chunks differ in content at position %v: %02x vs %02x", count, bufA[0], bufB[0])
		}
		if errA == io.EOF && errB == io.EOF {
			break
		}
		count++
	}
	check(t, "closing file a in assertEqual", a.Close())
	check(t, "closing file b in assertEqual", b.Close())
}

func createSimilarData(tempA, tempB io.Writer, p, sigma, avgchunk float64, numchunks int) error {
	for numchunks > 0 {
		numchunks--
		lengthA := int(avgchunk*sigma*rand.NormFloat64() + avgchunk)
		if lengthA < 16 {
			lengthA = 16
		}
		data := randomBytes(lengthA)
		if _, err := tempA.Write(data); err != nil {
			return err
		}
		same := rand.Float64() <= p
		if same {
			if _, err := tempB.Write(data); err != nil {
				return err
			}
		} else {
			lengthB := int(avgchunk*sigma*rand.NormFloat64() + avgchunk)
			if lengthB < 16 {
				lengthB = 16
			}
			data = randomBytes(lengthB)
			if _, err := tempB.Write(data); err != nil {
				return err
			}
		}
	}
	return nil
}

func randomBytes(length int) []byte {
	result := make([]byte, 0, length)
	for len(result) < length {
		result = append(result, byte(rand.Int()))
	}
	return result
}

type testPrinter struct {
	t *testing.T
}

func (t *testPrinter) Printf(format string, v ...interface{}) {
	t.t.Logf(format, v...)
}

func reportUsage(t *testing.T, name string, store cafs.BoundedStorage) {
	store.FreeCache()
	ui := store.GetUsageInfo()
	if ui.Locked != 0 {
		t.Errorf("  Store %v: %v", name, ui)
		store.DumpStatistics(&testPrinter{t})
		t.FailNow()
	}
}
