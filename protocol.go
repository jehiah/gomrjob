package gomrjob

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-simplejson"
	"io"
	"log"
)

// returns a channel of simplejson.Json objects. This channel will be closed
// when the input stream closes. Errors will be logged
func JsonInputProtocol(input io.Reader) chan *simplejson.Json {
	out := make(chan *simplejson.Json, 100)
	go func() {
		var line []byte
		var err error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		for {
			if err == io.EOF {
				break
			}
			line, err = r.ReadBytes('\n')
			if len(line) == 0 {
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
	Values chan *simplejson.Json
}

// returns a channel of KeyJsonChan which includes the key, and a channel to read values
// Each channel will be closed when no data is finished. Errors will be logged
func JsonInternalInputProtocol(input io.Reader) chan JsonKeyChan {
	out := make(chan JsonKeyChan)
	var jsonChan chan *simplejson.Json
	go func() {
		var line []byte
		var err error
		r := bufio.NewReaderSize(input, 1024*1024*2)
		var lastKey []byte
		for {
			if err == io.EOF {
				break
			}
			line, err = r.ReadBytes('\n')
			if len(line) == 0 {
				continue
			}
			chunks := bytes.SplitAfterN(line, []byte("\t"), 2)
			if len(chunks) != 2 {
				log.Printf("invalid line. no tab - %s", line)
				Counter("JsonInternalInputProtocol", "invalid line - no tab", 1)
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

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

func RawKeyValueOutputProtocol(writer io.Writer) chan KeyValue {
	in := make(chan KeyValue)
	go func() {
		for kv := range in {
			fmt.Fprintf(writer, "%+v\t%+v\n", kv.Key, kv.Value)
		}
	}()
	return in
}

// a json Key, and a json value
func JsonInternalOutputProtocol(writer io.Writer) chan KeyValue {
	in := make(chan KeyValue)
	tab := []byte("\t")
	newline := []byte("\n")
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
			writer.Write(kBytes)
			writer.Write(tab)
			writer.Write(vBytes)
			writer.Write(newline)
		}
	}()
	return in
}
