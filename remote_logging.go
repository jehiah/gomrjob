package gomrjob

import (
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// redirect log to a remote port
func dialRemoteLogger(addr string) (io.Writer, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Duration(5)*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// listen for log messages, and copy them to stderr
func startRemoteLogListner() string {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "0.0.0.0:0")
	if err != nil {
		log.Fatalf("error resolving %s", err)
	}
	ln, err := net.Listen("tcp4", tcpAddr.String())
	if err != nil {
		log.Fatalf("error listening %s", err)
	}
	log.Printf("listening on %v for log messages", ln.Addr())

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// handle error
				continue
			}
			log.Printf("accepted remote logging connection from %s", conn.RemoteAddr())
			go io.Copy(os.Stderr, conn)
		}
	}()

	listenAddr := ln.Addr().String()
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed getting hostname %s", err)
	}

	return strings.Replace(listenAddr, "0.0.0.0", hostname, 1)
}

type prefixLogger struct {
	prefix []byte
	w      io.Writer
}

func (p *prefixLogger) Write(b []byte) (n int, err error) {
	n, err = p.w.Write(p.prefix)
	if err != nil {
		return n, err
	}
	nn, err := p.w.Write(b)
	return n + nn, err
}

// NewPrefixLogger returns a writer that behaves like w except
// that it writes a prefix before each write
func newPrefixLogger(prefix string, w io.Writer) io.Writer {
	return &prefixLogger{[]byte(prefix), w}
}
