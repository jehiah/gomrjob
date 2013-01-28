package main

import (
	"../"
	"flag"
	"io"
	"log"
	"fmt"
)

var (
	input = flag.String("input", "", "path to hdfs input file")
)

type MRStep struct {
}

func (s *MRStep) MapperSetup() error {
	return nil
}
func (s *MRStep) MapperTeardown(w io.Writer) error {
	return nil
}

// An example Map function. It consumes json data and yields a value for each line
func (s *MRStep) Mapper(r io.Reader, w io.Writer) error {
	out := gomrjob.RawKeyValueOutputProtocol(w)
	for data := range gomrjob.JsonInputProtocol(r) {
		gomrjob.Counter("example_mr", "map_lines_read", 1)
		key, err := data.Get("api_path").String()
		if err != nil {
			gomrjob.Counter("example_mr", "missing_key", 1)
		} else {
			out <- gomrjob.KeyValue{key, "1"}
		}
	}
	close(out)
	return nil
}

func (s *MRStep) ReducerSetup() error {
	return nil
}
func (s *MRStep) ReducerTeardown(w io.Writer) error {
	return nil
}

// A simple reduce function that counts keys
func (t *MRStep) Reducer(r io.Reader, w io.Writer) error {
	out := gomrjob.RawKeyValueOutputProtocol(w)
	for kv := range gomrjob.JsonInternalInputProtocol(r) {
		var i int64
		for v := range kv.Values {
			vv, err := v.Int64()
			if err != nil {
				log.Printf("non-int value %s", err)
			} else {
				i += vv
			}
		}
		out <- gomrjob.KeyValue{kv.Key, i}
	}
	close(out)
	return nil
}

func main() {
	flag.Parse()

	runner := gomrjob.NewRunner()
	runner.Name = "test-mr"
	runner.InputFiles = append(runner.InputFiles, *input)
	runner.Steps = append(runner.Steps, &MRStep{})
	err := runner.Run()
	if err != nil {
		gomrjob.Status(fmt.Sprintf("Run error %s", err))
		log.Fatalf("Run error %s", err)
	}

}
