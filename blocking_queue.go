package parallel_disk_io

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	errQueueStopped = errors.New("queue is stopped")
)

type BlockingQueue struct {
	mu       sync.Mutex
	condPush *sync.Cond
	condPop  *sync.Cond
	stopped  int32

	queue []DataBlock
	cap   int
}

func NewBlockingQueue(cap int) Queue {
	bq := &BlockingQueue{
		cap:     cap,
		queue:   make([]DataBlock, 0, cap),
		stopped: 0,
	}
	bq.condPush = sync.NewCond(&bq.mu)
	bq.condPop = sync.NewCond(&bq.mu)
	return bq
}

func (bq *BlockingQueue) GetCapacity() int {
	return bq.cap
}

func (bq *BlockingQueue) IsStopped() bool {
	return atomic.LoadInt32(&bq.stopped) == 1
}

func (bq *BlockingQueue) Push(blk DataBlock) error {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	for len(bq.queue) == bq.cap && !bq.IsStopped() {
		bq.condPop.Wait()
	}
	if bq.IsStopped() {
		return errQueueStopped
	}
	bq.queue = append(bq.queue, blk)

	bq.condPop.Signal()
	return nil
}

func (bq *BlockingQueue) Pop() (DataBlock, error) {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	for len(bq.queue) == 0 && !bq.IsStopped() {
		bq.condPop.Wait()
	}
	if bq.IsStopped() {
		return nil, errQueueStopped
	}
	blk := bq.queue[0]
	bq.queue = bq.queue[1:]

	bq.condPush.Signal()
	return blk, nil
}

func (bq *BlockingQueue) Stop() {
	atomic.StoreInt32(&bq.stopped, 1)
	bq.condPop.Broadcast()
	bq.condPush.Broadcast()
}

var _ Queue = &BlockingQueue{}
