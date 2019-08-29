package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/indyjo/cafs"
	"github.com/indyjo/cafs/ram"
	"github.com/indyjo/cafs/remotesync"
	"github.com/indyjo/cafs/remotesync/httpsync"
	"io"
	"log"
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
			log.Fatalf("Error loading '[v]: %v", preload, err)
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
			w.Write([]byte("No such profile"))
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

	handler := httpsync.NewFileHandlerFromFile(file)
	fileHandlers[file.Key().String()] = handler

	path = fmt.Sprintf("/file/%v", file.Key())
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

type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (f flushWriter) Write(p []byte) (n int, err error) {
	return f.w.Write(p)
}

func (f flushWriter) Flush() {
}

func syncFile(fileStorage cafs.FileStorage, source string) error {
	log.Printf("Sync from %v", source)
	resp, err := http.Get(source)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET returned status %v", resp.Status)
	}
	var syncinfo remotesync.SyncInfo
	err = json.NewDecoder(resp.Body).Decode(&syncinfo)
	if err != nil {
		return err
	}

	builder := remotesync.NewBuilder(storage, &syncinfo, 32, "synced from "+source)
	defer builder.Dispose()

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, source, pr)
	if err != nil {
		return err
	}

	go func() {
		err := builder.WriteWishList(flushWriter{
			w: pw,
			f: nil,
		})
		if err != nil {
			log.Printf("Error in WriteWishList: %v", err)
		}
		_ = pw.Close()
	}()

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	file, err := builder.ReconstructFileFromRequestedChunks(res.Body)
	if err != nil {
		return err
	}

	log.Printf("Successfully received %v (%v bytes)", file.Key(), file.Size())
	return nil
}
