package cache

import (
	"github.com/tikv/pd/pkg/btree"
	"sync"
)

const defaultDegree = 4

// PriorityQueue is a queue that supports priorities and preemption, and is thread-safe.
type PriorityQueue struct {
	items    map[uint64]*Entry
	btree    *btree.BTreeG[*Entry]
	capacity int
	mutex    sync.Mutex
}

// NewPriorityQueue constructs a new instance of a thread-safe priority queue.
func NewPriorityQueue(capacity int) *PriorityQueue {
	return &PriorityQueue{
		items:    make(map[uint64]*Entry),
		btree:    btree.NewG[*Entry](defaultDegree),
		capacity: capacity,
	}
}

// PriorityQueueItem avoid convert cost
type PriorityQueueItem interface {
	ID() uint64
}

// Put inserts a value with a given priority into the queue.
func (pq *PriorityQueue) Put(priority int, value PriorityQueueItem) bool {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	id := value.ID()
	entry, ok := pq.items[id]
	if !ok {
		entry = &Entry{Priority: priority, Value: value}
		if pq.Len() >= pq.capacity {
			min, found := pq.btree.Min()
			// avoid to capacity equal 0
			if !found || !min.Less(entry) {
				return false
			}
			pq.Remove(min.Value.ID())
		}
	} else if entry.Priority != priority { // delete before update
		pq.btree.Delete(entry)
		entry.Priority = priority
	}

	pq.btree.ReplaceOrInsert(entry)
	pq.items[id] = entry
	return true
}

// Get retrieves an entry by ID from the queue.
func (pq *PriorityQueue) Get(id uint64) *Entry {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	return pq.items[id]
}

// Peek returns the highest priority entry without removing it.
func (pq *PriorityQueue) Peek() *Entry {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if max, ok := pq.btree.Max(); ok {
		return max
	}
	return nil
}

// Tail returns the lowest priority entry without removing it.
func (pq *PriorityQueue) Tail() *Entry {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if min, ok := pq.btree.Min(); ok {
		return min
	}
	return nil
}

// Elems returns all elements in the queue.
func (pq *PriorityQueue) Elems() []*Entry {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	rs := make([]*Entry, pq.Len())
	count := 0
	pq.btree.Descend(func(i *Entry) bool {
		rs[count] = i
		count++
		return true
	})
	return rs
}

// Remove deletes an entry from the queue.
func (pq *PriorityQueue) Remove(id uint64) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if v, ok := pq.items[id]; ok {
		pq.btree.Delete(v)
		delete(pq.items, id)
	}
}

// Len returns the number of elements in the queue.
func (pq *PriorityQueue) Len() int {
	// Lock is not necessary for calling Len() on pq.btree as it is assumed to be thread-safe.
	return pq.btree.Len()
}

// Entry represents a priority queue entry.
type Entry struct {
	Priority int
	Value    PriorityQueueItem
}

// Less determines if the entry has a smaller priority than another.
func (r *Entry) Less(other *Entry) bool {
	return r.Priority > other.Priority
}
