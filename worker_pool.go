package parallel_disk_io

import (
	"context"
	"errors"
)

type WorkerType int

const (
	ReadWorker WorkerType = iota
	ProcessWorker
	BlockingWorker
)

/*
WorkerPool works like a semaphore with 3 category. All workers start in
Read Stat because we want to start fast.
*/
type WorkerPool struct {
	workerLimit int

	availiableWorkers []chan int
}

func NewWorkerPool(limit int) (*WorkerPool, error) {
	if limit < 0 {
		return nil, errors.New("negative number of workers not supported")
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
		availiableWorkers: availiableWorkers,
	}, nil
}

func (wp *WorkerPool) CheckoutWorker(ctx context.Context, t WorkerType) (int, error) {
	select {
	case <-ctx.Done():
		return -1, errors.New("request worker timeout")
	case wid := <-wp.availiableWorkers[t]:
		return wid, nil

	}
}

func (wp *WorkerPool) CheckinWorker(wid int, t WorkerType) {
	wp.availiableWorkers[t] <- wid
}
