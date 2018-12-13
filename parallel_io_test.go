package parallel_disk_io

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ParallelDiskReadAndProcessSuite struct {
	suite.Suite

	tmpFile      *os.File
	testFilePath string
}

func (s *ParallelDiskReadAndProcessSuite) SetupSuite() {
	randomByte := make([]byte, 6400*100)
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
			BlockSize:     64,
			WorkerLimit:   10,
			MinReadWorker: 0,
		},
	)
	assert.Nil(s.T(), err)
	err = pdrp.ReadAndProcess(s.tmpFile)
	assert.Nil(s.T(), err)
}

func TestParallelDiskReadAndProcessSuite(t *testing.T) {
	suite.Run(t, new(ParallelDiskReadAndProcessSuite))
}
