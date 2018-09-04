package mrproto

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/bitly/go-simplejson"
	"github.com/jehiah/gomrjob"
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
				gomrjob.Counter("JsonInputProtocol", "invalid line", 1)
				log.Printf("%s - failed parsing %s", err, line)
			} else {
				out <- data
			}
		}
		close(out)
	}()
	return out
}

// returns a channel of []byte's. This channel will be closed
// when the input stream closes. Errors will be logged
func RawInputProtocol(input io.Reader) <-chan []byte {
	out := make(chan []byte, 100)
	go func() {
		var line []byte
		var lineErr error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		for {
			if lineErr == io.EOF {
				break
			}
			if lineErr != nil {
				log.Printf("%s - failed parsing %q", lineErr, line)
				break
			}
			line, lineErr = r.ReadBytes('\n')
			if len(line) < 1 || lineErr != nil {
				continue
			}
			out <- line
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
				gomrjob.Counter("JsonInternalInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			if !bytes.Equal(chunks[0], lastKey) {
				if jsonChan != nil {
					close(jsonChan)
					jsonChan = nil
				}
				key, err := simplejson.NewJson(chunks[0])
				if err != nil {
					gomrjob.Counter("JsonInternalInputProtocol", "invalid line", 1)
					log.Printf("%s - failed parsing key %s", err, line)
					continue
				}
				lastKey = chunks[0]

				jsonChan = make(chan *simplejson.Json, 100)
				out <- JsonKeyChan{key, jsonChan}
			}
			data, err := simplejson.NewJson(chunks[1])
			if err != nil {
				gomrjob.Counter("JsonInternalInputProtocol", "invalid line", 1)
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
				gomrjob.Counter("RawJsonInternalInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			if !bytes.Equal(chunks[0], lastKey) || jsonChan == nil {
				if jsonChan != nil {
					close(jsonChan)
					jsonChan = nil
				}
				lastKey = chunks[0]
				jsonChan = make(chan *simplejson.Json, 100)
				out <- RawJsonKeyChan{lastKey, jsonChan}
			}
			data, err := simplejson.NewJson(chunks[1])
			if err != nil {
				gomrjob.Counter("RawJsonInternalInputProtocol", "invalid line", 1)
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
	out := make(chan KeyValue, 100)
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
				gomrjob.Counter("RawInternalInputProtocol", "invalid line - no tab", 1)
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
	w := bufio.NewWriter(writer)
	in := make(chan KeyValue, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			fmt.Fprintf(w, "%+v\t%+v\n", kv.Key, kv.Value)
		}
		w.Flush()
		wg.Done()
	}()
	return &wg, in
}

// a json Key, and a json value
func JsonInternalOutputProtocol(writer io.Writer) (*sync.WaitGroup, chan<- KeyValue) {
	w := bufio.NewWriter(writer)
	in := make(chan KeyValue, 100)
	tab := []byte("\t")
	newline := []byte("\n")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			kBytes, err := json.Marshal(kv.Key)
			if err != nil {
				gomrjob.Counter("JsonInternalOutputProtocol", "unable to json encode key", 1)
				log.Printf("%s - failed encoding %v", err, kv.Key)
				continue
			}
			vBytes, err := json.Marshal(kv.Value)
			if err != nil {
				gomrjob.Counter("JsonInternalOutputProtocol", "unable to json encode value", 1)
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
	in := make(chan KeyValue, 100)
	tab := []byte("\t")
	newline := []byte("\n")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for kv := range in {
			kBytes, ok := kv.Key.([]byte)
			if !ok {
				gomrjob.Counter("RawJsonInternalOutputProtocol", "key is not []byte", 1)
				log.Printf("failed type casting %v", kv.Key)
				continue
			}
			vBytes, err := json.Marshal(kv.Value)
			if err != nil {
				gomrjob.Counter("RawJsonInternalOutputProtocol", "unable to json encode value", 1)
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

type RawKeyChan struct {
	Key    []byte
	Values <-chan []byte
}

// a raw Key and a channel of Raw Values
func RawInternalChanInputProtocol(input io.Reader) <-chan RawKeyChan {
	out := make(chan RawKeyChan)
	var innerChan chan []byte
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
				gomrjob.Counter("RawInternalChanInputProtocol", "invalid line - no tab", 1)
				log.Printf("invalid line. no tab - %s", line)
				lastKey = lastKey[:0]
				continue
			}
			if !bytes.Equal(chunks[0], lastKey) || innerChan == nil {
				if innerChan != nil {
					close(innerChan)
					innerChan = nil
				}
				lastKey = chunks[0]
				innerChan = make(chan []byte, 100)
				out <- RawKeyChan{lastKey, innerChan}
			}
			innerChan <- chunks[1]
		}
		if innerChan != nil {
			close(innerChan)
		}
		close(out)
	}()
	return out
}
