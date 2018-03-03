package main

func (pq maxHeap) Len() int { return len(pq) }

func (pq maxHeap) Less(i, j int) bool {
	return pq[i].distance > pq[j].distance
}

func (pq maxHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *maxHeap) Push(x interface{}) {
	item := x.(*Node)
	*pq = append(*pq, item)
}

func (pq *maxHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0: n-1]
	return item
}
