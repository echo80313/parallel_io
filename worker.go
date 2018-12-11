package parallel_disk_io

type WorkerState int

const (
	WorkerStateRead WorkerState = iota
	WorkerStateProcess
	WorkerStateBlock
)

type Worker struct {
	state WorkerState
}
