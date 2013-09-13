package gomrjob

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-simplejson"
	"io"
	"log"
	"sync"
)

// returns a channel of simplejson.Json objects. This channel will be closed
// when the input stream closes. Errors will be logged
func JsonInputProtocol(input io.Reader) <-chan *simplejson.Json {
	out := make(chan *simplejson.Json, 100)
	go func() {
		var line []byte
		var lineErr error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		for {
			if lineErr == io.EOF {
				break
			}
			line, lineErr = r.ReadBytes('\n')
			if len(line) <= 1 {
				continue
			}
			data, err := simplejson.NewJson(line)
			if err != nil {
				Counter("JsonInputProtocol", "invalid line", 1)
				log.Printf("%s - failed parsing %s", err, line)
			} else {
				out <- data
			}
		}
		close(out)
	}()
	return out
}

type JsonKeyChan struct {
	Key    *simplejson.Json
	Values <-chan *simplejson.Json
}

// returns an input channel with a simplejson.Json key, and a channel of simplejson.Json Values which includes the key
// Each channel will be closed when no data is finished. Errors will be logged
func JsonInternalInputProtocol(input io.Reader) <-chan JsonKeyChan {
	out := make(chan JsonKeyChan)
	var jsonChan chan *simplejson.Json
	go func() {
		var line []byte
		var lineErr error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		var lastKey []byte
		for {
			if lineErr == io.EOF {
				break
			}
			line, lineErr = r.ReadBytes('\n')
			if len(line) <= 1 {
				continue
			}
			chunks := bytes.SplitN(line, []byte("\t"), 2)
			if len(chunks) != 2 {
				Counter("JsonInternalInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			if !bytes.Equal(chunks[0], lastKey) {
				if jsonChan != nil {
					close(jsonChan)
				}
				key, err := simplejson.NewJson(chunks[0])
				if err != nil {
					Counter("JsonInternalInputProtocol", "invalid line", 1)
					log.Printf("%s - failed parsing key %s", err, line)
					continue
				}
				lastKey = chunks[0]

				jsonChan = make(chan *simplejson.Json, 100)
				out <- JsonKeyChan{key, jsonChan}
			}
			data, err := simplejson.NewJson(chunks[1])
			if err != nil {
				Counter("JsonInternalInputProtocol", "invalid line", 1)
				log.Printf("%s - failed parsing %s", err, line)
			} else {
				jsonChan <- data
			}
		}
		if jsonChan != nil {
			close(jsonChan)
		}
		close(out)
	}()
	return out
}

type RawJsonKeyChan struct {
	Key    []byte
	Values <-chan *simplejson.Json
}

// returns an input channel with a simplejson.Json key, and a channel of simplejson.Json Values which includes the key
// Each channel will be closed when no data is finished. Errors will be logged
func RawJsonInternalInputProtocol(input io.Reader) <-chan RawJsonKeyChan {
	out := make(chan RawJsonKeyChan)
	var jsonChan chan *simplejson.Json
	go func() {
		var line []byte
		var lineErr error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		var lastKey []byte
		for {
			if lineErr == io.EOF {
				break
			}
			line, lineErr = r.ReadBytes('\n')
			if len(line) <= 1 {
				continue
			}
			chunks := bytes.SplitN(line, []byte("\t"), 2)
			if len(chunks) != 2 {
				Counter("RawJsonInternalInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			if !bytes.Equal(chunks[0], lastKey) {
				if jsonChan != nil {
					close(jsonChan)
				}
				lastKey = chunks[0]
				jsonChan = make(chan *simplejson.Json, 100)
				out <- RawJsonKeyChan{lastKey, jsonChan}
			}
			data, err := simplejson.NewJson(chunks[1])
			if err != nil {
				Counter("RawJsonInternalInputProtocol", "invalid line", 1)
				log.Printf("%s - failed parsing %s", err, line)
			} else {
				jsonChan <- data
			}
		}
		if jsonChan != nil {
			close(jsonChan)
		}
		close(out)
	}()
	return out
}

// returns an input channel with a raw key, value without collating keys
func RawInternalInputProtocol(input io.Reader) <-chan KeyValue {
	out := make(chan KeyValue)
	go func() {
		var line []byte
		var lineErr error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		var lastKey []byte
		for {
			if lineErr == io.EOF {
				break
			}
			line, lineErr = r.ReadBytes('\n')
			if len(line) <= 1 {
				continue
			}
			chunks := bytes.SplitN(line, []byte("\t"), 2)
			if len(chunks) != 2 {
				Counter("RawInternalInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			out <- KeyValue{chunks[0], chunks[1]}
		}
		close(out)
	}()
	return out
}

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

func RawKeyValueOutputProtocol(writer io.Writer) (*sync.WaitGroup, chan<- KeyValue) {
	in := make(chan KeyValue)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			fmt.Fprintf(writer, "%+v\t%+v\n", kv.Key, kv.Value)
		}
		wg.Done()
	}()
	return &wg, in
}

// a json Key, and a json value
func JsonInternalOutputProtocol(writer io.Writer) (*sync.WaitGroup, chan<- KeyValue) {
	w := bufio.NewWriter(writer)
	in := make(chan KeyValue)
	tab := []byte("\t")
	newline := []byte("\n")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			kBytes, err := json.Marshal(kv.Key)
			if err != nil {
				Counter("JsonInternalOutputProtocol", "unable to json encode key", 1)
				log.Printf("%s - failed encoding %v", err, kv.Key)
				continue
			}
			vBytes, err := json.Marshal(kv.Value)
			if err != nil {
				Counter("JsonInternalOutputProtocol", "unable to json encode value", 1)
				log.Printf("%s - failed encoding %v", err, kv.Value)
				continue
			}
			w.Write(kBytes)
			w.Write(tab)
			w.Write(vBytes)
			w.Write(newline)
		}
		w.Flush()
		wg.Done()
	}()
	return &wg, in
}

// a raw byte Key, and a json value
func RawJsonInternalOutputProtocol(writer io.Writer) (*sync.WaitGroup, chan<- KeyValue) {
	w := bufio.NewWriter(writer)
	in := make(chan KeyValue)
	tab := []byte("\t")
	newline := []byte("\n")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			kBytes, ok := kv.Key.([]byte)
			if !ok {
				Counter("RawJsonInternalOutputProtocol", "key is not []byte", 1)
				log.Printf("failed type casting %v", kv.Key)
				continue
			}
			vBytes, err := json.Marshal(kv.Value)
			if err != nil {
				Counter("RawJsonInternalOutputProtocol", "unable to json encode value", 1)
				log.Printf("%s - failed encoding %v", err, kv.Value)
				continue
			}
			w.Write(kBytes)
			w.Write(tab)
			w.Write(vBytes)
			w.Write(newline)
		}
		w.Flush()
		wg.Done()
	}()
	return &wg, in
}
