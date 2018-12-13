package parallel_disk_io

import (
	"context"
	"errors"
	"os"
	"sync"
)

type Params struct {
	WorkerLimit        int
	MinReadWorker      int
	BlockSize          int64
	QueueHighWaterMark int
	QueueLowWaterMark  int
	QueueCap           int
}

type ParallelDiskReadAndProcess struct {
	workerLimit        int
	workerPool         *WorkerPool
	minReadWorker      int
	dataQueue          Queue
	blockSize          int64
	queueHighWaterMark int
	queueLowWaterMark  int
}

func NewParallelDiskReadAndProcess(params Params) (*ParallelDiskReadAndProcess, error) {
	wp, err := NewWorkerPool(params.WorkerLimit)
	if err != nil {
		return nil, err
	}
	return &ParallelDiskReadAndProcess{
		workerLimit:        params.WorkerLimit,
		workerPool:         wp,
		minReadWorker:      params.MinReadWorker,
		blockSize:          params.BlockSize,
		dataQueue:          NewBlockingQueue(params.QueueCap),
		queueHighWaterMark: params.QueueHighWaterMark,
		queueLowWaterMark:  params.QueueLowWaterMark,
	}, nil
}

func (p *ParallelDiskReadAndProcess) ReadAndProcess(file *os.File, process func(int, DataBlock)) error {
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
	for i := 0; i < numberOfChunks; i++ {
		wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ReadWorker)
		go func(blkID int64) {
			file.ReadAt(buffer[wid], blkID*p.blockSize)
			process(int(blkID), buffer[wid])
			wg.Done()
		}(int64(i))
		p.workerPool.CheckinWorker(wid, ReadWorker)
	}
	wg.Wait()

	return nil
}
