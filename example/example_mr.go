package main

import (
	"../"
	"flag"
	"io"
	"log"
)

type MapStep struct {
	
}

func (m *MapStep) Run(r io.Reader, w io.Writer) error {
	for data := range gomrjob.JsonInputProtocol(r) {
		log.Printf("got %v", data)
		gomrjob.Counter("example_mr", "map_lines_read", 1)
	}
	return nil
}

type ReduceStep struct {
	
}

func (r *ReduceStep) Run(io.Reader, io.Writer) error {
	return nil
}

func main() {
	flag.Parse()

	runner := gomrjob.NewRunner()
	runner.Mapper = &MapStep{}
	runner.Reducer = &ReduceStep{}
	runner.Run()

}