package parallel_disk_io

type DataBlock []byte

type Queue interface {
	Push(DataBlock) error
	Pop() (DataBlock, error)
	Stop()
}
