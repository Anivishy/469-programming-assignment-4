package common

type TaskType string

const (
	MapTask TaskType = "map"
	ReduceTask TaskType = "reduce"
	ReplicateTask TaskType = "replicate"
	WaitTask TaskType = "wait"
	DoneTask TaskType = "done"
)

// heartbeat api

type HeartBeatRequest struct {
	WorkerID int
	WorkerAddr string
	CurrentTaskType TaskType
	CurrentTaskID int
}

type HeartBeatResponse struct {
	Acknowledged bool
	CoordinatorDone bool
}

// task api

type TaskRequest struct {
	WorkerID int
	WorkerAddr string
}

type IntermediateFileRef struct {
	MapTaskID int
	ReduceID int
	FileName string
	WorkerID int
	WorkerAddr string
}

type TaskResponse struct {
	TaskType TaskType
	TaskID int
	NumMap int
	NumReduce int
	BatchUrls []string
	FileNames []string
	IntermediateFiles []IntermediateFileRef
	OwnerWorker int
	ReplicaFile string
	ReplicaReduceID int
	ReplicaDestinationWorker int
	ReplicaDestinationAddr string
}

type TaskDoneRequest struct {
	WorkerID int
	WorkerAddr string
	TaskType TaskType
	TaskID int
	DiscoveredUrls []string
	OutputFiles []string
	TaskSuccess bool
	ErrorMessage string
	MissingInputs []IntermediateFileRef
}

type TaskDoneResponse struct {
	Complete bool
}

// search api
type SearchRequest struct {
	ClientID int
	ThreadID int
	Keyword string
}

type SearchResponse struct {
	Ready bool
	WorkerID int
	WorkerAddr string
	FileName string
}

type WorkerSearchRequest struct {
	Keyword string
	FileName string
}

type WorkerSearchResponse struct {
	URLs []string
}

// worker to worker file transfer
type IntermediateTransferRequest struct {
	FromWorkerID int
	MapTaskID int
	ReduceID int
	Data string
}

type IntermediateTransferResponse struct {
	Stored bool
}

type ReplicaTransferRequest struct {
	FromWorkerID int
	ReduceID int
	FileName string
	Data string
}

type ReplicaTransferResponse struct {
	Stored bool
}
