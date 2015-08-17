package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"github.com/indyjo/cafs/chunking"
	"io"
	"os"
	"sort"
)

const APP_VERSION = "0.1"

// The flag package provides a default help printer via -h switch
var versionFlag = flag.Bool("v", false, "Print the version number.")
var numFingers = flag.Int("n", 5, "Number of fingers in handprint.")
var matrixMode = flag.Bool("m", false, "Display similarity matrix.")
var printChunks = flag.Bool("c", false, "Print chunks on the go.")

func main() {
	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}

	fingerprints := make(map[string]bool)

	for _, arg := range flag.Args() {
		if handprint, err := chunkFile(arg, *numFingers, !*matrixMode, *printChunks); err != nil {
			fmt.Println("Failed: ", err)
		} else {
			for _, fingerprint := range handprint.Fingerprints {
				fingerprints[fmt.Sprintf("%16x", fingerprint[:8])] = true
			}
		}
	}

	if *matrixMode {
		allFingers := make([]string, 0, len(fingerprints))
		for k, _ := range fingerprints {
			allFingers = append(allFingers, k)
		}
		sort.Strings(allFingers)
		for _, arg := range flag.Args() {
			if handprint, err := chunkFile(arg, *numFingers, false, false); err != nil {
				fmt.Println("Failed: ", err)
			} else {
				fingerprintsInHandprint := make(map[string]bool)
				for _, fingerprint := range handprint.Fingerprints {
					fingerprintsInHandprint[fmt.Sprintf("%16x", fingerprint[:8])] = true
				}
				row := make([]byte, 0, 32)
				for _, fingerprint := range allFingers {
					if _, ok := fingerprintsInHandprint[fingerprint]; ok {
						row = append(row, '.')
					} else {
						row = append(row, ' ')
					}
				}
				fmt.Printf("%s %s\n", row, arg)
			}
		}
	}
}

type Handprint struct {
	Fingerprints [][]byte
}

func NewHandprint(size int) *Handprint {
	return &Handprint{make([][]byte, 0, size)}
}

func (h *Handprint) String() string {
	result := make([]byte, 0, 20)
	for _, v := range h.Fingerprints {
		result = append(result, []byte(fmt.Sprintf("%4x", v[:2]))...)
	}
	return string(result)
}

func (h *Handprint) Insert(fingerprint []byte) {
	for i, other := range h.Fingerprints {
		if bytes.Compare(other, fingerprint) <= 0 {
			continue
		}
		fingerprint, h.Fingerprints[i] = other, fingerprint
	}
	if len(h.Fingerprints) < cap(h.Fingerprints) {
		h.Fingerprints = append(h.Fingerprints, fingerprint)
	}
}

func chunkFile(filename string, size int, printSummary, printChunks bool) (*Handprint, error) {
	handprint := NewHandprint(size)
	//fmt.Printf("Chunking %s\n", filename)
	fi, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	buffer := make([]byte, 16384)
	chunker := chunking.New()

	numChunks := 1
	numBytes := 0
	sha := sha256.New()
	chunkLen := 0
	for {
		n, err := fi.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			handprint.Insert(sha.Sum(make([]byte, 0, 32)))
			break
		}
		numBytes += n
		slice := buffer[:n]
		for len(slice) > 0 {
			bytesInChunk := chunker.Scan(slice)
			chunkLen += bytesInChunk
			sha.Write(slice[:bytesInChunk])
			if bytesInChunk < len(slice) {
				handprint.Insert(sha.Sum(make([]byte, 0, 32)))
				if printChunks {
					fmt.Printf(" %6d %032x\n", chunkLen, sha.Sum(make([]byte, 0, 32)))
				}
				sha.Reset()
				chunkLen = 0
				numChunks++
			}
			slice = slice[bytesInChunk:]
		}
	}

	//fmt.Printf("Generated %d chunks on avg %d bytes long.\n", numChunks, numBytes/numChunks)
	if printSummary {
		fmt.Printf("%-20s %s\n", handprint, filename)
	}
	return handprint, nil
}
