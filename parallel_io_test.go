package parallel_disk_io

import (
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ParallelDiskReadAndProcessSuite struct {
	suite.Suite

	tmpFile      *os.File
	testFilePath string
	blkSize      int
	contentDict  []DataBlock
	alphaSetSize int
}

func (s *ParallelDiskReadAndProcessSuite) generateFileContent(n int) []byte {
	alphaSet := []byte("abcdefghijklmnopqrstuvwxyz")
	s.alphaSetSize = len(alphaSet)
	s.contentDict = make([]DataBlock, s.alphaSetSize)
	for i := 0; i < s.alphaSetSize; i++ {
		s.contentDict[i] = make([]byte, s.blkSize)
		for j := 0; j < s.blkSize; j++ {
			s.contentDict[i][j] = alphaSet[i]
		}
	}

	content := make([]byte, n*s.blkSize)
	for i := 0; i < n; i++ {
		copy(content[i*s.blkSize:i*s.blkSize+s.blkSize], s.contentDict[i%26])
	}
	return content
}

func (s *ParallelDiskReadAndProcessSuite) SetupSuite() {
	s.blkSize = 64
	randomByte := s.generateFileContent(1000) // 1MiB
	s.testFilePath = "parallel.test"
	ioutil.WriteFile(s.testFilePath, randomByte, 0666)
	s.tmpFile, _ = os.Open(s.testFilePath)
}

func (s *ParallelDiskReadAndProcessSuite) TearDownSuite() {
	s.tmpFile.Close()
	os.Remove(s.testFilePath)
}

func (s *ParallelDiskReadAndProcessSuite) TestBasicRead() {
	pdrp, err := NewParallelDiskReadAndProcess(
		Params{
			BlockSize:     int64(s.blkSize),
			WorkerLimit:   10,
			MinReadWorker: 1,
			NumOfCPU:      4,
			QueueCap:      10,
		},
	)
	assert.Nil(s.T(), err)
	processedBlks := int32(0)
	err = pdrp.ReadAndProcess(s.tmpFile, func(b DataBlock) {
		atomic.AddInt32(&processedBlks, 1)
	})
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), int32(1000), atomic.LoadInt32(&processedBlks))
}

func TestParallelDiskReadAndProcessSuite(t *testing.T) {
	suite.Run(t, new(ParallelDiskReadAndProcessSuite))
}
