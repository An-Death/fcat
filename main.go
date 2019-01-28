package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type records [][]byte

var (
	addr         = flag.String("addr", "", "UDP Address to send data")
	file         = flag.String("file", "", "Path to file. If path end with .gz - data will be compressed")
	timeout      = flag.Duration("timeout", 0, `Timeout between send messages. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`)
	scenarioFile string
	scenario     records
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
	scenarioFile = flag.Arg(0)
	if scenarioFile == "" {
		log.Fatal("Scenario file for load testing required")
	}

	scenario = readScenarioFromFile(scenarioFile)
	w := startWorker()
	go func() {
		w.Work(scenario)
		globalEnd = time.Since(globalStart)
		done <- true
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill, syscall.SIGTERM)

	<-quit
	globalEnd = time.Since(globalStart)
	w.Close()

	fmt.Printf("Time: %s,  Total messages: %v, Dropped: %v\n", globalEnd, totalLines, droppedLines)
	fmt.Printf("Message per sec: %v\n", float64(totalLines)/globalEnd.Seconds())
	os.Exit(0)
}

func startWorker() *worker {
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

func readScenarioFromFile(f string) [][]byte {
	fd, err := os.OpenFile(f, os.O_RDONLY, 0660)
	if err != nil {
		log.Fatalf("Error open scenario file")
	}
	defer fd.Close()
	buf := make([][]byte, 1024)

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		buf = append(buf, scanner.Bytes())
	}
	return buf

}
