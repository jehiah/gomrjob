// Copyright 2013 Lars Buitinck
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

// Counter with eviction for least-recently used (LRU) items.
// orignal LRU code at https://gist.github.com/larsmans/4638795

package gomrjob

// a LRU counter that calls a function when an item is removed
type LRUCounter struct {
	RemovalFunc func(interface{}, int64)
	index       map[interface{}]int // index of key in queue
	queue       list
}

// Create a new LRU cache for function f with the desired capacity.
func NewLRUCounter(removalFunc func(interface{}, int64), capacity int) *LRUCounter {
	if capacity < 1 {
		panic("capacity < 1")
	}
	c := &LRUCounter{RemovalFunc: removalFunc, index: make(map[interface{}]int)}
	c.queue.init(capacity)
	return c
}

// // Fetch value for key in the cache, calling Func to compute it if necessary.
// func (c *LRUCounter) Get(key interface{}) (value interface{}) {
//     i, stored := c.index[key]
//     if stored {
//         value = c.queue.valueAt(i)
//         c.queue.moveToFront(i)
//     } else {
//         value = c.Func(key)
//         c.insert(key, value)
//     }
//     return value
// }

// Number of items currently in the cache.
func (c *LRUCounter) Len() int {
	return len(c.queue.links)
}

func (c *LRUCounter) Capacity() int {
	return cap(c.queue.links)
}

// Iterate over the cache in LRU order. Useful for debugging.
func (c *LRUCounter) Iter(keys chan interface{}, values chan interface{}) {
	for i := c.queue.tail; i != -1; {
		n := c.queue.links[i]
		keys <- n.key
		values <- n.value
		i = n.next
	}
	close(keys)
	close(values)
}

func (c *LRUCounter) Flush() {
	for i := c.queue.tail; i != -1; {
		n := c.queue.links[i]
		c.RemovalFunc(n.key, n.value)
		i = n.next
	}
	c.index = make(map[interface{}]int)
	c.queue.init(cap(c.queue.links))
}

func (c *LRUCounter) Incr(key interface{}, value int64) {
	i, stored := c.index[key]
	if stored {
		c.queue.incrAt(i, value)
		return
	}
	q := &c.queue
	if q.full() {
		// evict least recently used item
		var k interface{}
		var v int64
		i, k, v = q.popTail()
		c.RemovalFunc(k, v)
		delete(c.index, k)
	} else {
		i = q.grow()
	}
	q.putFront(key, value, i)
	c.index[key] = i
}

// Doubly linked list containing key/value pairs.
type list struct {
	front, tail, empty int
	links              []link
}

type link struct {
	key        interface{}
	value      int64
	prev, next int
}

// Initialize l with capacity c.
func (l *list) init(c int) {
	l.front = -1
	l.tail = -1
	l.links = make([]link, 0, c)
}

func (l *list) full() bool {
	return len(l.links) == cap(l.links)
}

// Grow list by one element and return its index.
func (l *list) grow() (i int) {
	i = len(l.links)
	l.links = l.links[:i+1]
	return
}

// // Make the node at position i the front of the list.
// // Precondition: the list is not empty.
// func (l *list) moveToFront(i int) {
//     nf := &l.links[i]
//     of := &l.links[l.front]
// 
//     nf.prev = l.front
//     of.next = i
//     l.front = i
// }

// Pop the tail off the list and return its index and its key.
// Precondition: the list is full.
func (l *list) popTail() (i int, key interface{}, value int64) {
	i = l.tail
	t := &l.links[i]
	key = t.key
	value = t.value
	l.links[t.next].prev = -1
	l.tail = t.next
	return
}

// Put (key, value) in position i and make it the front of the list.
func (l *list) putFront(key interface{}, value int64, i int) {
	f := &l.links[i]
	f.key = key
	f.value = value
	f.prev = l.front
	f.next = -1

	if l.tail == -1 {
		l.tail = i
	} else {
		l.links[l.front].next = i
	}
	l.front = i
}

func (l *list) incrAt(i int, d int64) {
	l.links[i].value += d
}
