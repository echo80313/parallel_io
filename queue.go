package parallel_disk_io

type DataBlock []byte

type Queue interface {
	Length() int
	IsStopped() bool
	Push(DataBlock) error
	Pop() (DataBlock, error)
	Stop()
}
