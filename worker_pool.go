package parallel_disk_io

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type WorkerType int

const (
	ReadWorker WorkerType = iota
	ProcessWorker
	BlockingWorker
)

/*
WorkerPool works like a semaphore with 3 categories. All workers start in
Read Stat because we want to start fast.
*/
type WorkerPool struct {
	workerLimit int
	minRead     int
	numCPU      int

	checkOutCount []int
	mu            sync.Mutex
	que           Queue

	availiableWorkers []chan int
}

func NewWorkerPool(limit, minRead, numCPU int, q Queue) (*WorkerPool, error) {
	if limit < 2 || minRead < 1 || numCPU < 0 || limit-minRead < 1 {
		return nil, fmt.Errorf(
			"invalid params to create worker pool:(limit: %d, minRead: %d, numCPU: %d)",
			limit,
			minRead,
			numCPU,
		)
	}
	availiableWorkers := make([]chan int, 3)
	for i := 0; i < 3; i++ {
		availiableWorkers[i] = make(chan int, limit)
	}
	for i := 0; i < limit; i++ {
		availiableWorkers[ReadWorker] <- i
	}

	return &WorkerPool{
		workerLimit:       limit,
		minRead:           minRead,
		numCPU:            numCPU,
		checkOutCount:     make([]int, 3),
		availiableWorkers: availiableWorkers,
		que:               q,
	}, nil
}

// CheckoutWorker acquire worker wid from t state
func (wp *WorkerPool) CheckoutWorker(ctx context.Context, t WorkerType) (int, error) {
	select {
	case <-ctx.Done():
		return -1, errors.New("request worker timeout")
	case wid := <-wp.availiableWorkers[t]:
		wp.mu.Lock()
		wp.checkOutCount[t]++
		wp.mu.Unlock()
		return wid, nil
	}
}

// CheckinWorker returns worker wid to t state. wid is the id of worker,
// and t is the type of worker being used. It's caller's responsibility
// to provide the correct information.
func (wp *WorkerPool) CheckinWorker(wid int, t WorkerType) {
	if wid < 0 || wid >= wp.workerLimit {
		// invalid id, do nothing
		return
	}
	wp.mu.Lock()
	wp.checkOutCount[t]--
	nActiveRead := wp.checkOutCount[ReadWorker]
	nActiveRrocess := wp.checkOutCount[ProcessWorker]
	wp.mu.Unlock()
	var toCheckinType WorkerType
	switch t {
	case ReadWorker:
		if nActiveRrocess < wp.numCPU {
			toCheckinType = ProcessWorker
			wp.activateWorker()
		} else if nActiveRead < wp.minRead || nActiveRead < nActiveRrocess {
			toCheckinType = ReadWorker
		} else {
			toCheckinType = BlockingWorker
		}
	case ProcessWorker:
		if nActiveRrocess < wp.numCPU && wp.que.Length() > 0 {
			toCheckinType = ProcessWorker
			wp.activateWorker()
		} else if nActiveRead < wp.minRead || nActiveRead < nActiveRrocess {
			toCheckinType = ReadWorker
		} else {
			toCheckinType = BlockingWorker
		}
	case BlockingWorker:
		toCheckinType = BlockingWorker
	}
	wp.availiableWorkers[toCheckinType] <- wid
}

// ActivateWorker moves a worker in BLOCK state if any to READ state.
func (wp *WorkerPool) activateWorker() {
	select {
	case wid := <-wp.availiableWorkers[BlockingWorker]:
		wp.availiableWorkers[ReadWorker] <- wid
	default:
	}
}
