package parallel_disk_io

import (
	"context"
	"errors"
	"io"
	"os"
)

type ProcessFunc func(DataBlock) error

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

func (p *ParallelDiskReadAndProcess) ReadAndProcess(
	file *os.File,
	process ProcessFunc) error {
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

	errChan := make(chan error, numberOfChunks)
	go func() {
		for !p.dataQueue.IsStopped() {
			blk, err := p.dataQueue.Pop()
			if err != nil {
				errChan <- err
				continue
			}
			wid, err := p.workerPool.CheckoutWorker(context.TODO(), ProcessWorker)
			if err != nil {
				errChan <- err
			}
			go func(id int, blk DataBlock) {
				err := process(blk)
				p.workerPool.CheckinWorker(wid, ProcessWorker)
				errChan <- err
			}(wid, blk)
		}
	}()

	for i := 0; i < numberOfChunks; i++ {
		wid, _ := p.workerPool.CheckoutWorker(context.TODO(), ReadWorker)
		go func(blkID int64) {
			_, err := file.ReadAt(buffer[wid], blkID*p.blockSize)
			if err == nil || err == io.EOF {
				p.dataQueue.Push(buffer[wid])
			} else {
				// we are not going to process this blk, so it's done.
				errChan <- err
			}
			p.workerPool.CheckinWorker(wid, ReadWorker)
		}(int64(i))
	}
	for i := 0; i < numberOfChunks; i++ {
		err2 := <-errChan
		// report the frist error we encountered.
		if err2 != nil && err == nil {
			err = err2
		}
	}

	return err
}
