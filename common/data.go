package common

type TaskType string

const (
    MapTask TaskType = "map"
    ReduceTask TaskType = "reduce"
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
