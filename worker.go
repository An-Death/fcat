package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

const gz = ".gz"

type worker struct {
	writer   io.WriteCloser
	closable []io.Closer
	timeout  time.Duration
}

func (w *worker) Work(fileChan chan string) {
	for file := range fileChan {
		w.WorkOnce(file)
	}
	w.Close()
}

func (w *worker) WorkOnce(file string) {
	scan, err := w.readFile(file)
	if err != nil {
		log.Printf("Worker : %s\n", err)
		return
	}

	if w.timeout == 0 {
		w.noTimeout(scan)
	} else {
		w.withTimeout(scan, w.timeout)
	}
}

func (w *worker) noTimeout(scanner *bufio.Scanner) {
	for scanner.Scan() {
		w.sendLine(scanner.Bytes())
	}
}

func (w *worker) withTimeout(scanner *bufio.Scanner, timeout time.Duration) {
	t := time.Tick(timeout)
	for scanner.Scan() {
		w.sendLine(scanner.Bytes())
		<-t
	}
}

func (w *worker) sendLine(data []byte) {
	if _, err := w.writer.Write(append(data, '\n')); err == nil {
		totalLines++
	} else {
		droppedLines++
	}
}

func (w *worker) Close() {
	w.writer.Close()
	for _, cl := range w.closable {
		cl.Close()
	}
}

func (w *worker) readFile(name string) (*bufio.Scanner, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	w.appendClosable(file)

	if filepath.Ext(name) == gz {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		w.appendClosable(gz)
		return bufio.NewScanner(gz), nil
	}
	return bufio.NewScanner(file), nil
}

func (w *worker) appendClosable(cl io.Closer) {
	w.closable = append(w.closable, cl)
}

func getUDPConnection(addr string) (net.Conn, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type F struct {
	f  *os.File
	gf *gzip.Writer
	fw *bufio.Writer
}

func (f *F) Write(p []byte) (n int, err error) {
	return (f.fw).Write(p)
}

func (f *F) Close() error {
	f.fw.Flush()
	f.gf.Close()
	f.f.Close()
	return nil
}

func getGzFile(path string) (f *F, err error) {
	fi, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	gf := gzip.NewWriter(fi)
	fw := bufio.NewWriter(gf)
	f = &F{fi, gf, fw}
	return f, nil
}
