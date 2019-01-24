package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

var (
	addr         = flag.String("addr", "", "UDP Address to send data")
	file         = flag.String("file", "", "Path to file. If path end with .gz - data will be compressed")
	timeout      = flag.Duration("timeout", 0, `Timeout between send messages. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`)
	pathToFiles  string
	workedFiles  int
	totalFiles   int
	droppedLines int
	totalLines   int
	globalStart  time.Time
	globalEnd    time.Duration
	done         = make(chan bool)
)

func init() {
	runtime.GOMAXPROCS(1)
}

func main() {
	globalStart = time.Now()
	flag.Parse()

	pathToFiles = flag.Args()[0]
	fmt.Printf("Args: %s\n", flag.Args())

	// count Total
	filepath.Walk(pathToFiles, countFiles)
	fmt.Println("Total files: ", totalFiles)

	w := startWorkers()
	go func() {
		filepath.Walk(pathToFiles, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			w.WorkOnce(path)
			workedFiles++
			fmt.Printf("%v/%v of total\t\r", workedFiles, totalFiles)
			return nil
		})
		globalEnd = time.Since(globalStart)
		done <- true
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case <-quit:
		globalEnd = time.Since(globalStart)
		log.Println("Wait worker done with current file")
		w.Close()
	case <-done:
		w.Close()
		fmt.Println("All files done!")
		break
	}
	fmt.Printf("Time: %s,  Total lines: %v, Dropped: %v\n", globalEnd, totalLines, droppedLines)
	fmt.Printf("Message per sec: %v\n", float64(totalLines)/globalEnd.Seconds())
	os.Exit(0)
}

func startWorkers() *worker {
	var writer io.WriteCloser
	var err error
	if *addr != "" {
		writer, err = getUDPConnection(*addr)
	} else if *file != "" {
		writer, err = getGzFile(*file)
	} else {
		log.Fatal("No UDP address or FILE specified")
	}
	if err != nil {
		log.Fatalf("Error with create writer: %s", err)
	}

	return &worker{writer: writer, timeout: *timeout}
}

func countFiles(_ string, _ os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	totalFiles++
	return nil
}
