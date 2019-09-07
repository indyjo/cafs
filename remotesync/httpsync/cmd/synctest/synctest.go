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
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.package main

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/ram"
	"github.com/indyjo/cafs/remotesync"
	"github.com/indyjo/cafs/remotesync/httpsync"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime/pprof"
)

var storage cafs.FileStorage = ram.NewRamStorage(1 << 30)
var fileHandlers = make(map[string]*httpsync.FileHandler)

func main() {
	addr := ":8080"
	flag.StringVar(&addr, "l", addr, "which port to listen to")

	preload := ""
	flag.StringVar(&preload, "i", preload, "input file to load")

	flag.BoolVar(&remotesync.LoggingEnabled, "enable-remotesync-logging", remotesync.LoggingEnabled,
		"enables detailed logging from the remotesync algorithm")

	flag.Parse()

	if preload != "" {
		if err := loadFile(storage, preload); err != nil {
			log.Fatalf("Error loading '[%v]: %v", preload, err)
		}
	}

	http.HandleFunc("/load", handleLoad)
	http.HandleFunc("/sync", handleSyncFrom)
	http.HandleFunc("/stackdump", func(w http.ResponseWriter, r *http.Request) {
		name := r.FormValue("name")
		if len(name) == 0 {
			name = "goroutine"
		}
		profile := pprof.Lookup(name)
		if profile == nil {
			_, _ = w.Write([]byte("No such profile"))
			return
		}
		err := profile.WriteTo(w, 1)
		if err != nil {
			log.Printf("Error in profile.WriteTo: %v\n", err)
		}
	})

	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatalf("Error in ListenAndServe: %v", err)
	}
}

func loadFile(storage cafs.FileStorage, path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}

	tmp := storage.Create(path)
	defer tmp.Dispose()
	n, err := io.Copy(tmp, f)
	if err != nil {
		return fmt.Errorf("error after copying %v bytes: %v", n, err)
	}

	err = tmp.Close()
	if err != nil {
		return
	}

	file := tmp.File()
	defer file.Dispose()
	log.Printf("Read file: %v (%v bytes, chunked: %v, %v chunks)", path, n, file.IsChunked(), file.NumChunks())

	printer := log.New(os.Stderr, "", log.LstdFlags)
	handler := httpsync.NewFileHandlerFromFile(file, rand.Perm(256)).WithPrinter(printer)
	fileHandlers[file.Key().String()] = handler

	path = fmt.Sprintf("/file/%v", file.Key().String()[:16])
	http.Handle(path, handler)
	log.Printf("  serving under %v", path)
	return
}

func handleLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	path := r.FormValue("path")
	if err := loadFile(storage, path); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleSyncFrom(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	source := r.FormValue("source")
	if err := syncFile(storage, source); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func syncFile(fileStorage cafs.FileStorage, source string) error {
	log.Printf("Sync from %v", source)
	if file, err := httpsync.SyncFrom(context.Background(), fileStorage, http.DefaultClient, source, "synced from "+source); err != nil {
		return err
	} else {
		log.Printf("Successfully received %v (%v bytes)", file.Key(), file.Size())
		file.Dispose()
	}
	return nil
}
