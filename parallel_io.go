package parallel_disk_io

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

type Params struct {
	WorkerLimit        int
	MinReadWorker      int
	BlockSize          int64
	QueueHighWaterMark int
	QueueLowWaterMark  int
	QueueCap           int
	NumOfCPU           int
}

type ParallelDiskReadAndProcess struct {
	workerLimit        int
	workerPool         *WorkerPool
	minReadWorker      int
	dataQueue          Queue
	blockSize          int64
	queueHighWaterMark int
	queueLowWaterMark  int
	numOfCPU           int
}

func NewParallelDiskReadAndProcess(params Params) (*ParallelDiskReadAndProcess, error) {
	wp, err := NewWorkerPool(params.WorkerLimit)
	if err != nil {
		return nil, err
	}
	if params.MinReadWorker < 1 {
		return nil, errors.New("min read workers should be at least 1")
	}
	if params.WorkerLimit-params.MinReadWorker < 1 {
		return nil, errors.New("should at least have 1 process worker")
	}
	if params.NumOfCPU == 0 {
		params.NumOfCPU = runtime.NumCPU()
	}
	return &ParallelDiskReadAndProcess{
		workerLimit:        params.WorkerLimit,
		workerPool:         wp,
		minReadWorker:      params.MinReadWorker,
		blockSize:          params.BlockSize,
		dataQueue:          NewBlockingQueue(params.QueueCap),
		queueHighWaterMark: params.QueueHighWaterMark,
		queueLowWaterMark:  params.QueueLowWaterMark,
		numOfCPU:           params.NumOfCPU,
	}, nil
}

func (p *ParallelDiskReadAndProcess) ReadAndProcess(file *os.File, process func(DataBlock)) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return errors.New("no file info availiable")
	}

	if !fileInfo.Mode().IsRegular() {
		return errors.New("only regular allowed")
	}

	fileSize := fileInfo.Size()
	numberOfChunks := int(fileSize / p.blockSize)
	if fileSize%p.blockSize != 0 {
		numberOfChunks++
	}

	buffer := make([][]byte, p.workerLimit)
	for i := 0; i < p.workerLimit; i++ {
		buffer[i] = make([]byte, p.blockSize)
	}

	var wg sync.WaitGroup
	wg.Add(numberOfChunks)

	nProcessWorker := int32(0)
	go func() {
		for !p.dataQueue.IsStopped() {
			blk, err := p.dataQueue.Pop()
			if err != nil {
				continue
			}
			wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ProcessWorker)
			atomic.AddInt32(&nProcessWorker, 1)
			go func(id int, blk DataBlock) {
				process(blk)
				if atomic.LoadInt32(&nProcessWorker) < int32(p.numOfCPU) && p.dataQueue.Length() > 0 {
					p.workerPool.CheckinWorker(wid, ProcessWorker)
				} else {
					p.workerPool.CheckinWorker(wid, ReadWorker)
				}
				atomic.AddInt32(&nProcessWorker, -1)
				wg.Done()
			}(wid, blk)
		}
	}()

	nReadWorker := int32(0)
	for i := 0; i < numberOfChunks; i++ {
		wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ReadWorker)
		atomic.AddInt32(&nReadWorker, 1)
		go func(blkID int64) {
			file.ReadAt(buffer[wid], blkID*p.blockSize)
			p.dataQueue.Push(buffer[wid])
			if atomic.LoadInt32(&nReadWorker) >= int32(p.minReadWorker) &&
				atomic.LoadInt32(&nProcessWorker) < int32(p.numOfCPU) {
				p.workerPool.CheckinWorker(wid, ProcessWorker)
			} else {
				p.workerPool.CheckinWorker(wid, ReadWorker)
			}
			atomic.AddInt32(&nReadWorker, -1)
		}(int64(i))
	}
	wg.Wait()
	p.dataQueue.Stop()

	return nil
}
