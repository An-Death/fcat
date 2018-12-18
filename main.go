package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"sync"
)

var (
	workers     = flag.Int("workers", 1, "Amount of workers to process the files")
	addr        = flag.String("addr", "localhost:8888", "UDP Address to send data")
	pathToFiles string
	fileChan    chan string
	workerPool  []*worker
	group       sync.WaitGroup
	workedFiles int
	totalFiles  int
)

func main() {
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

	close(fileChan)
	group.Wait()
	fmt.Println("All files done")

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
