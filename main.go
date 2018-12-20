package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

var (
	workers      = flag.Int("workers", 1, "Amount of workers to process the files")
	addr         = flag.String("addr", "localhost:8888", "UDP Address to send data")
	pathToFiles  string
	fileChan     chan string
	workerPool   []*worker
	group        sync.WaitGroup
	workedFiles  int
	totalFiles   int
	droppedLines int
	totalLines   int
)

func main() {
	start := time.Now()
	flag.Parse()
	fileChan = make(chan string, *workers)

	pathToFiles = flag.Args()[0]
	fmt.Printf("Args: %s\n", flag.Args())
	// count Total
	walkDir(pathToFiles, func(fl string) {
		totalFiles++
	})
	fmt.Println("Total files: ", totalFiles)
	startWorkers()

	walkDir(pathToFiles, func(filename string) {
		fileChan <- filename
		workedFiles++
		fmt.Printf("%v/%v of total\t\r", workedFiles, totalFiles)
	})
	fmt.Println("\nWait workers...")
	close(fileChan)
	group.Wait()
	fmt.Printf("All files done! Time: %s,  Total lines: %v, Dropped: %v", time.Since(start), totalLines, droppedLines)
	os.Exit(0)
}

func startWorkers() {
	workerPool = make([]*worker, *workers)

	for i := 0; i != *workers; i++ {

		conn, err := getUDPConnection(*addr)
		if err != nil {
			closeWorkers(workerPool)
			close(fileChan)
			log.Fatal(err)
		}
		w := worker{id: i, con: conn, group: &group}
		go w.Work(fileChan)
		workerPool[i] = &w
	}
}

func closeWorkers(pool []*worker) {
	for _, w := range pool {
		if w != nil {
			w.Close()
		}
	}
}

func walkDir(dir string, apply func(string)) {
	var fullPath = func(part string) string {
		return path.Join(pathToFiles, part)
	}

	files, err := ioutil.ReadDir(pathToFiles)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() {
			continue
			//walkDir(fullPath(f.Name()), apply)
		} else {
			apply(fullPath(f.Name()))
		}
	}
}
