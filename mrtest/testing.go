package mrtest

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"sync"
	"testing"

	"github.com/jehiah/gomrjob"
)

type dataLines [][]byte

func (s dataLines) Len() int      { return len(s) }
func (s dataLines) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type sortedData struct{ dataLines }

func (s sortedData) Less(i, j int) bool { return bytes.Compare(s.dataLines[i], s.dataLines[j]) == -1 }

// a simple line sort (handling missing trailing newline on input)
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
		_, err := out.Write(bytes.TrimRight(line, "\n"))
		if err != nil {
			return err
		}
		_, err = out.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func runReduceStep(t *testing.T, s gomrjob.Step, in io.Reader) []byte {
	// TODO: test the combiner (if present)
	// in -> map -> sort -> reduce -> out
	var wg sync.WaitGroup

	sortIn, mapOut := io.Pipe()
	var sortInReader io.Reader
	sortInReader = sortIn
	reduceIn, sortOut := io.Pipe()
	reduceOut := bytes.NewBuffer([]byte{})
	wg.Add(2)
	if _, ok := s.(gomrjob.Mapper); ok {
		wg.Add(1)
		go func() {
			err := s.(gomrjob.Mapper).Mapper(in, mapOut)
			if err != nil {
				t.Errorf("mapper failed with %s", err)
			}
			mapOut.Close()
			wg.Done()
		}()
	} else {
		log.Printf("skipping mapper")
		sortInReader = in
	}
	go func() {
		err := sortPhase(sortInReader, sortOut)
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
	result := bytes.TrimSpace(reduceOut.Bytes())
	return result
}

func TestMapReduceSteps(t *testing.T, ss []gomrjob.Step, in io.Reader, out io.Reader) []byte {
	var result []byte
	for i, s := range ss {
		if i == 0 {
			result = runReduceStep(t, s, in)
		} else {
			result = runReduceStep(t, s, bytes.NewBuffer(result))
		}
	}
	outBytes, err := ioutil.ReadAll(out)
	if err != nil {
		t.Errorf("failed reading expected output %s", err)
		return nil
	}
	outBytes = bytes.TrimSpace(outBytes)
	if !bytes.Equal(result, outBytes) {
		// TODO: iterate line by line for better feedback on errors
		t.Logf("got output:\n%s", result)
		t.Logf("expected output:\n%s", outBytes)
		t.Errorf("output does not match expected output")
	}
	return result
}

// test that a geven step, and input generates a given output
func TestMapReduceStep(t *testing.T, s gomrjob.Step, in io.Reader, out io.Reader) []byte {
	outBytes, err := ioutil.ReadAll(out)
	if err != nil {
		t.Errorf("failed reading expected output %s", err)
		return nil
	}
	outBytes = bytes.TrimSpace(outBytes)
	result := runReduceStep(t, s, in)
	if !bytes.Equal(result, outBytes) {
		// TODO: iterate line by line for better feedback on errors
		t.Logf("got output:\n%s", result)
		t.Logf("expected output:\n%s", outBytes)
		t.Errorf("output does not match expected output")
	}
	return result
}
