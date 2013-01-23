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

func (s *MapStep) Run(r io.Reader, w io.Writer) error {
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

type ReduceStep struct {
}

func (s *ReduceStep) Run(r io.Reader, w io.Writer) error {
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
	runner.Input = *input
	runner.Name = "test-mr"
	runner.Mapper = &MapStep{}
	runner.Reducer = &ReduceStep{}
	err := runner.Run()
	if err != nil {
		log.Fatalf("Run error %s", err)
	}

}
