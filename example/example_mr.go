package main

import (
	"../"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

var (
	input = flag.String("input", "", "path to hdfs input file")
)

type MRStep struct {
}

// An example Map function. It consumes json data and yields a value for each line
func (s *MRStep) Mapper(r io.Reader, w io.Writer) error {
	wg, out := gomrjob.JsonInternalOutputProtocol(w)
	for data := range gomrjob.JsonInputProtocol(r) {
		gomrjob.Counter("example_mr", "map_lines_read", 1)
		key, err := data.Get("api_path").String()
		if err != nil {
			gomrjob.Counter("example_mr", "missing_key", 1)
		} else {
			out <- gomrjob.KeyValue{key, 1}
		}
	}
	close(out)
	wg.Wait()
	return nil
}

// just re-use the reducer as the combiner
func (s *MRStep) Combiner(r io.Reader, w io.Writer) error {
	return s.Reducer(r, w)
}

// A simple reduce function that counts keys
func (s *MRStep) Reducer(r io.Reader, w io.Writer) error {
	wg, out := gomrjob.JsonInternalOutputProtocol(w)
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
		keyString, err := kv.Key.String()
		if err != nil {
			log.Printf("non-string key %s", err)
		}
		out <- gomrjob.KeyValue{keyString, i}
	}
	close(out)
	wg.Wait()
	return nil
}

func main() {
	flag.Parse()

	runner := gomrjob.NewRunner()
	runner.Name = "test-gomrjob"
	runner.InputFiles = append(runner.InputFiles, *input)
	runner.Steps = append(runner.Steps, &MRStep{})
	err := runner.Run()
	if err != nil {
		gomrjob.Status(fmt.Sprintf("Run error %s", err))
		log.Fatalf("Run error %s", err)
	}
	gomrjob.Cat(os.Stdout, fmt.Sprintf("%s/part-*", runner.Output))
	// runner.Cleanup()

}
