package parallel_disk_io

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSinglePushAndPop(t *testing.T) {
	que := NewBlockingQueue(10)
	content := DataBlock([]byte("aaa"))

	err := que.Push(content)
	assert.Nil(t, err)

	blk, err := que.Pop()
	assert.Nil(t, err)
	assert.Equal(t, content, blk)
	que.Stop()
}

func TestMultiPushes(t *testing.T) {
	que := NewBlockingQueue(3)
	for i := 0; i < 3; i++ {
		err := que.Push(DataBlock([]byte("aaa")))
		assert.Nil(t, err)
	}

	pushDone := make(chan struct{})
	var pushErr error
	go func() {
		pushErr = que.Push(DataBlock([]byte("aaa")))
		close(pushDone)
	}()
	popDone := make(chan struct{})
	var popErr error
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, popErr = que.Pop()
		close(popDone)
	}()

	select {
	case <-pushDone:
		assert.Fail(t, "push should block until pop finished")
	case <-popDone:
		assert.Nil(t, popErr)
	}
	<-pushDone
	<-popDone
	assert.Nil(t, pushErr)
}

func TestMultiPopes(t *testing.T) {
	que := NewBlockingQueue(3)
	for i := 0; i < 3; i++ {
		err := que.Push(DataBlock([]byte("aaa")))
		assert.Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		blk, err := que.Pop()
		assert.Nil(t, err)
		assert.Equal(t, DataBlock([]byte("aaa")), blk)
	}

	popDone := make(chan struct{})
	var popErr error
	go func() {
		_, popErr = que.Pop()
		close(popDone)
	}()

	pushDone := make(chan struct{})
	var pushErr error
	go func() {
		time.Sleep(100 * time.Millisecond)
		pushErr = que.Push(DataBlock([]byte("aaa")))
		close(pushDone)
	}()

	select {
	case <-popDone:
		assert.Fail(t, "pop should block until push finished")
	case <-pushDone:
		assert.Nil(t, pushErr)
	}
	<-pushDone
	<-popDone
	assert.Nil(t, popErr)
}

func TestStop(t *testing.T) {
	que := NewBlockingQueue(10)
	content := DataBlock([]byte("aaa"))
	que.Push(content)

	que.Stop()
	err := que.Push(content)
	assert.NotNil(t, err)

	_, err = que.Pop()
	assert.NotNil(t, err)
}
