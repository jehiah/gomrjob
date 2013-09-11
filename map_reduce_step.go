package gomrjob

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"sort"
	"log"
	"sync"
	"testing"
)

type Mapper interface {
	Mapper(io.Reader, io.Writer) error
}

type Reducer interface {
	Reducer(io.Reader, io.Writer) error
}

type Combiner interface {
	Combiner(io.Reader, io.Writer) error
}

type Step interface {
	Reducer
}

type dataLines [][]byte

func (s dataLines) Len() int      { return len(s) }
func (s dataLines) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type sortedData struct{ dataLines }

func (s sortedData) Less(i, j int) bool { return bytes.Compare(s.dataLines[i], s.dataLines[j]) == -1 }

// a simple line sort
func sortPhase(in io.Reader, out io.Writer) error {
	var data [][]byte
	r := bufio.NewReader(in)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) >= 1 {
			data = append(data, line)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	sort.Sort(sortedData{data})
	for _, line := range data {
		out.Write(line)
	}
	return nil
}

// test that a geven step, and input generates a given output
func TestMapReduceStep(t *testing.T, s Step, in io.Reader, out io.Reader) []byte {
	// TODO: test the combiner (if present)
	// in -> map -> sort -> reduce -> out
	var wg sync.WaitGroup

	sortIn, mapOut := io.Pipe()
	reduceIn, sortOut := io.Pipe()
	reduceOut := bytes.NewBuffer([]byte{})
	wg.Add(3)
	go func() {
		err := s.(Mapper).Mapper(in, mapOut)
		if err != nil {
			t.Errorf("mapper failed with %s", err)
		}
		mapOut.Close()
		wg.Done()
	}()
	go func() {
		err := sortPhase(sortIn, sortOut)
		if err != nil {
			t.Errorf("sort failed with %s", err)
		}
		sortIn.Close()
		sortOut.Close()
		wg.Done()
	}()
	go func() {
		err := s.Reducer(reduceIn, reduceOut)
		if err != nil {
			t.Errorf("reduce failed with %s", err)
		}
		reduceIn.Close()
		wg.Done()
	}()
	wg.Wait()
	outBytes, err := ioutil.ReadAll(out)
	if err != nil {
		t.Errorf("failed reading expected output %s", err)
		return nil
	}
	outBytes = bytes.TrimSpace(outBytes)
	result := bytes.TrimSpace(reduceOut.Bytes())
	if !bytes.Equal(result, outBytes) {
		// TODO: iterate line by line for better feedback on errors
		log.Printf("got output:\n%s", result)
		log.Printf("expected output:\n%s", outBytes)
		t.Errorf("output does not match expected output")
	}
	return result
}
