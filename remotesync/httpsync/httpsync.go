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
	"encoding/json"
	"errors"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/remotesync"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
)

type FilesSource interface {
	NextFiles() (remotesync.Files, error)
	Dispose()
}

type FileHandler struct {
	m        sync.Mutex
	source   FilesSource
	syncinfo *remotesync.SyncInfo
	log      cafs.Printer
}

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

type fileBaseFileSource struct {
	file cafs.File
}

func (f fileBaseFileSource) NextFiles() (remotesync.Files, error) {
	file := f.file
	if file == nil {
		return nil, errors.New("File already disposed")
	}
	return remotesync.ChunksOfFile(file), nil
}

func (f fileBaseFileSource) Dispose() {
	file := f.file
	f.file = nil
	if file != nil {
		file.Dispose()
	}
}

func NewFileHandlerFromFile(file cafs.File) *FileHandler {
	result := &FileHandler{
		m:        sync.Mutex{},
		source:   fileBaseFileSource{file.Duplicate()},
		syncinfo: &remotesync.SyncInfo{},
		log:      cafs.NewWriterPrinter(os.Stderr), // TODO take parameter
	}
	result.syncinfo.SetPermutation(rand.Perm(256))
	result.syncinfo.SetChunksFromFile(file)
	return result
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
	chunks, err := handler.source.NextFiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = remotesync.WriteChunkData(chunks, 0, bufio.NewReader(r.Body), handler.syncinfo.Perm, w, nil)
	if err != nil {
		log.Print("Error in WriteChunkDate: %v", err)
		return
	}
}
