package remotesync

import (
	"bytes"
	"encoding/json"
	"github.com/indyjo/cafs"
	"testing"
)

func TestSyncInfoJSON(t *testing.T) {
	s := SyncInfo{}
	s.addChunk(cafs.SKey{11, 22, 33, 44, 55, 66, 77, 88}, 1337)
	s.addChunk(cafs.SKey{11, 22, 33, 44, 55, 66, 77, 88}, 1337)
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Error encoding: %v", err)
	}
	// t.Logf("%v", string(b))

	s2 := SyncInfo{}
	err = json.Unmarshal(b, &s2)
	if err != nil {
		t.Fatalf("Error decoding: %v", err)
	}

	b2, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Error encoding: %v", err)
	}

	if !bytes.Equal(b, b2) {
		t.Fatalf("Encoding differs")
	}
}
