// Copyright 2018-2026 The Olric Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dmap

import (
	"container/heap"
	"sync"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
)

const (
	// expirationBatchSize bounds the work done for one wake-up. A large batch of
	// keys that expire together must not monopolise a CPU.
	expirationBatchSize = 256

	// expirationMaxWait is only a safety net. Writes wake the worker when they
	// add an earlier deadline.
	expirationMaxWait = time.Hour

	// expirationMinWait avoids a busy loop when a deadline is already due but
	// more work remains in the queue.
	expirationMinWait = time.Millisecond
)

type expirationKey struct {
	dmap *DMap
	hkey uint64
}

type expirationItem struct {
	dmap     *DMap
	hkey     uint64
	deadline int64
	index    int
}

type expirationHeap []*expirationItem

func (h expirationHeap) Len() int { return len(h) }

func (h expirationHeap) Less(i, j int) bool {
	if h[i].deadline == h[j].deadline {
		return h[i].hkey < h[j].hkey
	}
	return h[i].deadline < h[j].deadline
}

func (h expirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *expirationHeap) Push(x interface{}) {
	item := x.(*expirationItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *expirationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

type expirationQueue struct {
	mu    sync.Mutex
	items expirationHeap
	byKey map[expirationKey]*expirationItem
	wake  chan struct{}
}

func newExpirationQueue() *expirationQueue {
	return &expirationQueue{
		byKey: make(map[expirationKey]*expirationItem),
		wake:  make(chan struct{}, 1),
	}
}

func (q *expirationQueue) schedule(dm *DMap, hkey uint64, ttl int64) {
	if q == nil {
		return
	}

	key := expirationKey{dmap: dm, hkey: hkey}
	q.mu.Lock()
	if ttl == 0 {
		q.removeLocked(key)
		q.mu.Unlock()
		return
	}

	var top int64
	if len(q.items) > 0 {
		top = q.items[0].deadline
	}
	if item, ok := q.byKey[key]; ok {
		item.deadline = ttl
		heap.Fix(&q.items, item.index)
	} else {
		item := &expirationItem{
			dmap:     dm,
			hkey:     hkey,
			deadline: ttl,
		}
		heap.Push(&q.items, item)
		q.byKey[key] = item
	}
	wake := top == 0 || ttl < top
	q.mu.Unlock()
	if wake {
		q.notify()
	}
}

func (q *expirationQueue) remove(dm *DMap, hkey uint64) {
	if q == nil {
		return
	}

	q.mu.Lock()
	q.removeLocked(expirationKey{dmap: dm, hkey: hkey})
	q.mu.Unlock()
}

func (q *expirationQueue) removeLocked(key expirationKey) {
	item, ok := q.byKey[key]
	if !ok {
		return
	}
	heap.Remove(&q.items, item.index)
	delete(q.byKey, key)
}

func (q *expirationQueue) removeDMap(dm *DMap) {
	if q == nil {
		return
	}

	q.mu.Lock()
	for key, item := range q.byKey {
		if key.dmap != dm {
			continue
		}
		heap.Remove(&q.items, item.index)
		delete(q.byKey, key)
	}
	q.mu.Unlock()
}

func (q *expirationQueue) popDue(now int64, limit int) ([]*expirationItem, int64) {
	if q == nil {
		return nil, 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var due []*expirationItem
	for len(q.items) > 0 && len(due) < limit && q.items[0].deadline <= now {
		item := heap.Pop(&q.items).(*expirationItem)
		delete(q.byKey, expirationKey{dmap: item.dmap, hkey: item.hkey})
		due = append(due, item)
	}

	var next int64
	if len(q.items) > 0 {
		next = q.items[0].deadline
	}
	return due, next
}

func (q *expirationQueue) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (s *Service) scheduleExpiration(dm *DMap, hkey uint64, ttl int64) {
	s.expirations.schedule(dm, hkey, ttl)
}

func (s *Service) expireKeysAtBackground() {
	defer s.wg.Done()

	timer := time.NewTimer(expirationMaxWait)
	defer timer.Stop()

	for {
		due, next := s.expirations.popDue(time.Now().UnixMilli(), expirationBatchSize)
		if len(due) > 0 {
			s.expireKeys(due)
			// One goroutine cannot consume all CPUs, but pause after a full batch
			// so a huge expiry wave does not monopolise its scheduler thread.
			if len(due) == expirationBatchSize {
				select {
				case <-time.After(expirationMinWait):
				case <-s.ctx.Done():
					return
				}
			}
			continue
		}

		wait := expirationMaxWait
		if next != 0 {
			wait = time.Until(time.UnixMilli(next))
			if wait < expirationMinWait {
				wait = expirationMinWait
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)

		select {
		case <-timer.C:
		case <-s.expirations.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) expireKeys(items []*expirationItem) {
	for _, item := range items {
		if s.ctx.Err() != nil {
			return
		}
		s.expireKey(item)
	}
}

func (s *Service) expireKey(item *expirationItem) {
	dm := item.dmap
	part := dm.getPartitionByHKey(item.hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if err != nil {
		return
	}

	f.RLock()
	ttl, err := f.storage.GetTTL(item.hkey)
	if err != nil {
		f.RUnlock()
		return
	}
	if ttl == 0 || time.Now().UnixMilli() < ttl {
		f.RUnlock()
		return
	}
	key, err := f.storage.GetKey(item.hkey)
	f.RUnlock()
	if err != nil {
		return
	}

	if _, err := dm.deleteExpiredKey(item.hkey, key, f); err != nil {
		s.log.V(3).Printf("[ERROR] Failed to delete expired key: %s on DMap: %s: %v",
			key, dm.name, err)
	}
}
