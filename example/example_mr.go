package main

import (
	"../"
	"flag"
	"io"
	"log"
)

var (
	input = flag.String("input", "", "path to hdfs input file")
)

type MapStep struct {
}

func (m *MapStep) Run(r io.Reader, w io.Writer) error {
	out := gomrjob.RawKeyValueOutputProtocol(w)
	for data := range gomrjob.JsonInputProtocol(r) {
		log.Printf("got %v", data)
		gomrjob.Counter("example_mr", "map_lines_read", 1)
		out <- gomrjob.RawKeyValue{"\"key\"", "1"}
	}
	close(out)
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
	runner.Input = *input
	runner.Name = "test-mr"
	runner.Mapper = &MapStep{}
	runner.Reducer = &ReduceStep{}
	err := runner.Run()
	if err != nil {
		log.Fatalf("Run error %s", err)
	}

}
