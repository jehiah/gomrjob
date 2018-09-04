package mrproto

import (
	"bytes"
	"io"
	"testing"

	"github.com/bmizerany/assert"
)

func TestJsonInputProtocol(t *testing.T) {
	// make sure the invalid json record is skipped
	// make sure that the valid records are handled in order
	input := bytes.NewBufferString(`{"_HEARTBEAT_":1359516282.66455, "row": 0}
not-json-data
{"row":1}`)
	count := 0
	for record := range JsonInputProtocol(input) {
		i, err := record.Get("row").Int()
		assert.Equal(t, err, nil)
		assert.Equal(t, count, i)
		count += 1
	}
	assert.Equal(t, count, 2)
}

func TestJsonInternalOutputProtocol(t *testing.T) {
	// test writing json keys and values
	var b []byte
	buf := bytes.NewBuffer(b)

	wg, out := JsonInternalOutputProtocol(buf)
	out <- KeyValue{"a", 1}
	out <- KeyValue{[]string{"b", "c"}, uint32(1)}
	close(out)
	wg.Wait()

	assert.Equal(t, buf.String(), `"a"	1
["b","c"]	1
`)

}

func TestRawInternalChanInputProtocol(t *testing.T) {
	type testCase struct {
		data   string
		keys   int
		values int
	}

	tests := []testCase{
		{"\tkey\n\tkey\n", 1, 2},
		{"a\tkey\na\tkey\n", 1, 2},
		{"a\tkey\nb\tkey\nc\tkey\n", 3, 3},
	}

	consume := func(r io.Reader) (keys int, values int) {
		for kv := range RawInternalChanInputProtocol(r) {
			keys++
			for _ = range kv.Values {
				values++
			}
		}
		return
	}

	for i, tc := range tests {
		keys, values := consume(bytes.NewBufferString(tc.data))
		if keys != tc.keys {
			t.Errorf("test[%d] got %d expected %d keys", i, keys, tc.keys)
		}
		if values != tc.values {
			t.Errorf("test[%d] got %d expected %d values", i, values, tc.values)
		}
	}
}
