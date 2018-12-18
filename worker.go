package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
)

const gz = ".gz"

type worker struct {
	id       int
	con      net.Conn
	group    *sync.WaitGroup
	closable []io.Closer
}

func (w *worker) Work(fileChan chan string) {
	w.group.Add(1)
	for file := range fileChan {
		w.WorkOnce(file)
	}
	w.Close()
}

func (w *worker) WorkOnce(file string) {
	scan, err := w.readFile(file)
	if err != nil {
		log.Printf("Worker#%v : %s\n", w.id, err)
		return
	}
	for scan.Scan() {
		line := append(scan.Bytes(), '\n')
		w.con.Write(line)
	}
}

func (w *worker) Close() {
	w.con.Close()
	for _, cl := range w.closable {
		cl.Close()
	}
	w.group.Done()
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
