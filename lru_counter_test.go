package gomrjob

import (
	"github.com/bmizerany/assert"
	"log"
	"testing"
)

func TestLRUCounter(t *testing.T) {
	var removed int64
	var total int64
	var removedKeys []string
	removalFunc := func(k interface{}, v int64) {
		log.Printf("removal %s %d", k, v)
		removed += 1
		total += v
		removedKeys = append(removedKeys, k.(string))
	}
	lru := NewLRUCounter(removalFunc, 4)
	lru.Incr("key1", 1)
	lru.Incr("key2", 1)
	lru.Incr("key2", 1)
	lru.Incr("key3", 1)
	lru.Incr("key3", 1)
	lru.Incr("key4", 1)
	lru.Incr("key5", 1)

	assert.Equal(t, removed, int64(1))
	lru.Flush()
	assert.Equal(t, removed, int64(5))
	assert.Equal(t, total, int64(7))
}
