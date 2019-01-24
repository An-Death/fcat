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
	limit        = flag.Float64("limit", 0, "Messages per second to send. Multiply 1000. 0 is No limits")
	pathToFiles  string
	workedFiles  int
	totalFiles   int
	droppedLines int
	totalLines   int
	done         = make(chan bool)
)

func init() {
	runtime.GOMAXPROCS(1)
}

func main() {
	start := time.Now()
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
		done <- true
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case <-quit:
		log.Println("Wait worker done with current file")
		w.Wait()
		w.Close()
	case <-done:
		w.Close()
		fmt.Println("All files done!")
		break
	}
	fmt.Printf("Time: %s,  Total lines: %v, Dropped: %v\n", time.Since(start), totalLines, droppedLines)
	fmt.Printf("Message per sec: %v\n", float64(totalLines)/time.Since(start).Seconds())
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

	return &worker{writer: writer, limit: *limit}
}

func countFiles(_ string, _ os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	totalFiles++
	return nil
}
