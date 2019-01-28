package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"net"
	"os"
	"time"
)

type worker struct {
	writer  io.WriteCloser
	timeout time.Duration
}

func (w *worker) Work(scenario records) {
	if w.timeout > 0 {
		for m := range withTimeout(cycle(scenario), w.timeout) {
			w.sendLine(m)
		}
	} else {
		for m := range cycle(scenario) {
			w.sendLine(m)
		}
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

func cycle(d [][]byte) <-chan []byte {
	c := make(chan []byte, len(d)*100)
	go func() {
		for {
			for _, m := range d {
				c <- m
			}
		}
	}()
	return c
}

func withTimeout(cc <-chan []byte, timeout time.Duration) <-chan []byte {
	c := make(chan []byte, len(cc))
	go func() {
		t := time.Tick(timeout)
		for m := range cc {
			c <- m
			<-t
		}
	}()

	return c
}
