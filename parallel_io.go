package parallel_disk_io

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
)

type ProcessFunc func(DataBlock)

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
	workerLimit int
	workerPool  *WorkerPool
	dataQueue   Queue
	blockSize   int64
}

func NewParallelDiskReadAndProcess(params Params) (*ParallelDiskReadAndProcess, error) {
	if params.MinReadWorker < 1 {
		return nil, errors.New("min read workers should be at least 1")
	}
	if params.WorkerLimit-params.MinReadWorker < 1 {
		return nil, errors.New("should at least have 1 process worker")
	}
	if params.NumOfCPU == 0 {
		params.NumOfCPU = runtime.NumCPU()
	}
	dataQueue := NewBlockingQueue(params.QueueCap)
	wp, err := NewWorkerPool(
		params.WorkerLimit,
		params.MinReadWorker,
		params.NumOfCPU,
		dataQueue,
	)
	if err != nil {
		return nil, err
	}

	return &ParallelDiskReadAndProcess{
		workerLimit: params.WorkerLimit,
		workerPool:  wp,
		blockSize:   params.BlockSize,
		dataQueue:   dataQueue,
	}, nil
}

func (p *ParallelDiskReadAndProcess) ReadAndProcess(file *os.File, process ProcessFunc) error {
	defer p.dataQueue.Stop()

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
	total := 0
	go func() {
		for !p.dataQueue.IsStopped() {
			blk, err := p.dataQueue.Pop()
			if err != nil {
				continue
			}
			total++
			wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ProcessWorker)
			go func(id int, blk DataBlock) {
				process(blk)
				p.workerPool.CheckinWorker(wid, ProcessWorker)
				wg.Done()
			}(wid, blk)
		}
	}()

	for i := 0; i < numberOfChunks; i++ {
		wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ReadWorker)
		go func(blkID int64) {
			_, err := file.ReadAt(buffer[wid], blkID*p.blockSize)
			if err == nil || err == io.EOF {
				p.dataQueue.Push(buffer[wid])
			}
			p.workerPool.CheckinWorker(wid, ReadWorker)
		}(int64(i))
	}
	wg.Wait()

	return nil
}
