// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

type Item struct {
	value    int
	priority float32
	index    int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	Items []*Item
	Pqids []int
}

func NewPriorityQueue(len int) PriorityQueue {
	return PriorityQueue{
		Items: make([]*Item, len),
		Pqids: make([]int, len),
	}
}

func (pq PriorityQueue) Len() int { return len(pq.Items) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.Items[i].priority > pq.Items[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.Items[i], pq.Items[j] = pq.Items[j], pq.Items[i]
	pq.Items[i].index = i
	pq.Items[j].index = j
	pq.Pqids[pq.Items[i].value] = i
	pq.Pqids[pq.Items[j].value] = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.Items)
	item := x.(*Item)
	item.index = n
	pq.Items = append(pq.Items, item)
	pq.Pqids = append(pq.Pqids, x.(*Item).value)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.Items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	pq.Items = old[0 : n-1]
	return item
}
