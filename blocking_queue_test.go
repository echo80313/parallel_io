package parallel_disk_io

import (
	"testing"

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
