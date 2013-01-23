package gomrjob

import (
	"io"
	"log"
	"net"
	"os"
	"strings"
)

// redirect log to a remote port
func dialRemoteLogger(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	log.SetOutput(conn)
	return nil
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
