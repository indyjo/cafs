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

// Package httpsync implements methods for requesting and serving files via CAFS
package httpsync

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/remotesync"
	"github.com/indyjo/cafs/remotesync/shuffle"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

// Struct FileHandler implements the http.Handler interface and serves a file over HTTP.
// The protocol used matches with function SyncFrom.
// Create using the New... functions.
type FileHandler struct {
	m        sync.Mutex
	source   chunksSource
	syncinfo *remotesync.SyncInfo
	log      cafs.Printer
}

// It is the owner's responsibility to correctly dispose of FileHandler instances.
func (handler *FileHandler) Dispose() {
	handler.m.Lock()
	s := handler.source
	handler.source = nil
	handler.syncinfo = nil
	handler.m.Unlock()
	if s != nil {
		s.Dispose()
	}
}

// Function NewFileHandlerFromFile creates a FileHandler that serves chunks of a File.
func NewFileHandlerFromFile(file cafs.File, perm shuffle.Permutation) *FileHandler {
	result := &FileHandler{
		m:        sync.Mutex{},
		source:   fileBasedChunksSource{file: file.Duplicate()},
		syncinfo: &remotesync.SyncInfo{Perm: perm},
		log:      cafs.NewWriterPrinter(ioutil.Discard),
	}
	result.syncinfo.SetChunksFromFile(file)
	return result
}

// Function NewFileHandlerFromSyncInfo creates a FileHandler that serves chunks as
// specified in a FileInfo. It doesn't necessarily require all of the chunks to be present
// and will block waiting for a missing chunk to become available.
// As a specialty, a FileHander created using this function needs not be disposed.
func NewFileHandlerFromSyncInfo(syncinfo *remotesync.SyncInfo, storage cafs.FileStorage) *FileHandler {
	result := &FileHandler{
		m: sync.Mutex{},
		source: syncInfoChunksSource{
			syncinfo: syncinfo,
			storage:  storage,
		},
		syncinfo: syncinfo,
		log:      cafs.NewWriterPrinter(ioutil.Discard),
	}
	return result
}

// Sets the FileHandler's log Printer.
func (handler *FileHandler) WithPrinter(printer cafs.Printer) *FileHandler {
	handler.log = printer
	return handler
}

func (handler *FileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if err := json.NewEncoder(w).Encode(handler.syncinfo); err != nil {
			handler.log.Printf("Error serving SyncInfo: R%v", err)
		}
		return
	} else if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	chunks, err := handler.source.GetChunks()
	if err != nil {
		handler.log.Printf("GetChunks() failed: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer chunks.Dispose()
	cb := func(bytesToTransfer, bytesTransferred int64) {
		handler.log.Printf("  skipped: %v transferred: %v", -bytesToTransfer, bytesTransferred)
	}
	handler.log.Printf("Calling WriteChunkData")
	defer handler.log.Printf("WriteChunkData finished")
	err = remotesync.WriteChunkData(chunks, 0, bufio.NewReader(r.Body), handler.syncinfo.Perm, w, cb)
	if err != nil {
		handler.log.Printf("Error in WriteChunkData: %v", err)
		return
	}
}

// Function SyncFrom uses an HTTP client to connect to some URL and download a fie into the
// given FileStorage.
func SyncFrom(ctx context.Context, storage cafs.FileStorage, client *http.Client, url, info string) (file cafs.File, err error) {
	// Fetch SyncInfo from remote
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET returned status %v", resp.Status)
	}
	var syncinfo remotesync.SyncInfo
	err = json.NewDecoder(resp.Body).Decode(&syncinfo)
	if err != nil {
		return
	}

	// Create Builder and establish a bidirectional POST connection
	builder := remotesync.NewBuilder(storage, &syncinfo, 32, info)
	defer builder.Dispose()

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, url, pr)
	if err != nil {
		return
	}

	// Enable cancelation
	req = req.WithContext(ctx)

	go func() {
		if err := builder.WriteWishList(nopFlushWriter{pw}); err != nil {
			_ = pw.CloseWithError(fmt.Errorf("error in WriteWishList: %v", err))
			return
		}
		_ = pw.Close()
	}()

	res, err := client.Do(req)
	if err != nil {
		return
	}
	file, err = builder.ReconstructFileFromRequestedChunks(res.Body)
	return
}
