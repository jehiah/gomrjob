package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jehiah/gomrjob"
	"github.com/jehiah/gomrjob/hdfs"
	"github.com/jehiah/gomrjob/mrproto"
	"github.com/jehiah/lru"
)

type JsonEntryCounter struct {
}

// An example Map function. It consumes json data and yields a value for each line
func (s *JsonEntryCounter) Mapper(r io.Reader, w io.Writer) error {
	log.Printf("map_input_file %s", os.Getenv("map_input_file"))
	wg, out := mrproto.JsonInternalOutputProtocol(w)

	// for efficient counting, use an in-memory counter that flushes the least recently used item
	// less Mapper output makes for faster sorting and reducing.
	counter := lru.NewLRUCounter(func(k interface{}, v int64) {
		out <- mrproto.KeyValue{k, v}
	}, 100)

	for line := range mrproto.RawInputProtocol(r) {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(line, &record); err != nil {
			gomrjob.Counter("example_mr", "Unmarshal Error", 1)
			log.Printf("%s", err)
			continue
		}
		gomrjob.Counter("example_mr", "Map Lines Read", 1)
		counter.Incr("lines_read", 1)
		for k, _ := range record {
			counter.Incr(k, 1)
		}
	}
	counter.Flush()
	close(out)
	wg.Wait()
	return nil
}

func (s *JsonEntryCounter) Reducer(r io.Reader, w io.Writer) error {
	return mrproto.Sum(r, w)
	return nil
}

func main() {
	input := flag.String("input", "", "path to input file")
	name := flag.String("name", "gomrjob-example", "job name")
	flag.Parse()

	runner := gomrjob.NewRunner()
	runner.Name = *name
	runner.InputFiles = append(runner.InputFiles, *input)
	runner.ReducerTasks = 2
	runner.Steps = append(runner.Steps, &JsonEntryCounter{})
	err := runner.Run()
	if err != nil {
		gomrjob.Status(fmt.Sprintf("Run error %s", err))
		log.Fatalf("Run error %s", err)
	}

	switch runner.JobType {
	case gomrjob.HDFS:
		cmd := hdfs.Cat(fmt.Sprintf("%s/part-*", runner.Output))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
	case gomrjob.Dataproc:
		log.Printf("output in %s/part-*", runner.Output)
	}

}
