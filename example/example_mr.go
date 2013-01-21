package main

import (
	"../"
	"flag"
	"io"
	"log"
)

type MapStep struct {
	
}
func (m *MapStep) Run(io.Reader, io.Writer) error {
	return nil
}

type ReduceStep struct {
	
}

func (r *ReduceStep) Run(io.Reader, io.Writer) error {
	return nil
}

func main() {
	flag.Parse()
	streamingJar, err := gomrjob.StreamingJar()
	log.Printf("%v %v", streamingJar, err)
	// gomrjob.Copy("/tmp/test.txt", "/users/jehiah/tmp/test.txt")
	

	// runner := gomrjob.NewRunner()
	// runner.Mapper = &MapStep{}
	// runner.Reducer = &ReduceStep{}
	// runner.Run()

}