package coordinator

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"distributed_search_engine/common"
)

const batchSize int = 100
const heartbeatTimeout = 30 * time.Second
const heartbeatCheckInterval = 10 * time.Second

type PendingTask struct {
	id                    int
	batchUrls             []string
	intermediateFiles     []common.IntermediateFileRef
	outputFiles           []string
	file                  string
	taskType              common.TaskType
	status                string
	ownerWorker           int
	destinationWorker     int
	destinationWorkerAddr string
	reduceID              int
}

type ReduceOutputState struct {
	reduceID            int
	fileName            string
	primaryWorker       int
	holders             map[int]bool
	pendingReplicaTasks map[int]bool
}

type CoordinatorAPI struct {
	Tasks               []PendingTask
	MapTasks            []PendingTask
	ReduceTasks         []PendingTask
	R                   int
	numWorkers          int
	MaxVisitedUrls      int
	VisitedUrlCount     int
	DesiredReplicaCount int
	CompleteTaskCount   int
	TasksComplete       bool
	currentPhase        common.TaskType
	frontier            []string
	seenUrls            map[string]bool
	workerAddresses     map[int]string
	workerHostPrefix    string
	workerPortBase      int
	lastHeartBeat       map[int]time.Time
	failedWorkers       map[int]bool
	ReduceOutputs       map[int]*ReduceOutputState
	mu                  sync.Mutex
}

func (coord *CoordinatorAPI) RequestTask(request common.TaskRequest, response *common.TaskResponse) error {
	return coord.GetWork(request, response)
}

func (coord *CoordinatorAPI) TaskDone(request common.TaskDoneRequest, response *common.TaskDoneResponse) error {
	return coord.CompleteWork(request, response)
}

func (coord *CoordinatorAPI) HeartBeat(request common.HeartBeatRequest, response *common.HeartBeatResponse) error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	coord.registerWorker(request.WorkerID, request.WorkerAddr)
	response.Acknowledged = true
	response.CoordinatorDone = coord.currentPhase == common.DoneTask
	return nil
}

func (coord *CoordinatorAPI) Search(request common.SearchRequest, response *common.SearchResponse) error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	response.Ready = coord.currentPhase == common.DoneTask
	if !response.Ready {
		return nil
	}

	keyword := strings.ToLower(strings.TrimSpace(request.Keyword))
	if keyword == "" {
		return nil
	}

	reduceID := partitionWord(keyword, coord.R)
	output := coord.ReduceOutputs[reduceID]
	if output == nil {
		return nil
	}

	workerID := coord.pickSearchWorker(output)
	if workerID == 0 {
		return nil
	}

	response.WorkerID = workerID
	response.WorkerAddr = coord.workerAddresses[workerID]
	response.FileName = output.fileName
	return nil
}

func (coord *CoordinatorAPI) GetWork(request common.TaskRequest, response *common.TaskResponse) error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	coord.registerWorker(request.WorkerID, request.WorkerAddr)
	coord.reassignTimedOutWorkers()

	*response = common.TaskResponse{
		TaskType:  common.WaitTask,
		NumMap:    len(coord.MapTasks),
		NumReduce: coord.R,
	}

	if coord.currentPhase == common.DoneTask {
		response.TaskType = common.DoneTask
		return nil
	}

	if coord.TasksComplete {
		switch coord.currentPhase {
		case common.MapTask:
			coord.initializeReduceTasks()
		case common.ReduceTask:
			coord.initializeReplicationTasks()
		case common.ReplicateTask:
			coord.appendMissingReplicationTasks()
			coord.updateCompletionState()
			if coord.TasksComplete && coord.replicationSatisfied() {
				coord.currentPhase = common.DoneTask
				response.TaskType = common.DoneTask
				return nil
			}
		}
	}

	for i := range len(coord.Tasks) {
		if coord.Tasks[i].status != "idle" || coord.Tasks[i].ownerWorker != request.WorkerID {
			continue
		}

		response.TaskType = coord.Tasks[i].taskType
		response.TaskID = coord.Tasks[i].id
		response.OwnerWorker = coord.Tasks[i].ownerWorker
		response.NumMap = len(coord.MapTasks)

		if coord.Tasks[i].taskType == common.MapTask {
			response.BatchUrls = append([]string(nil), coord.Tasks[i].batchUrls...)
			response.IntermediateFiles = coord.buildIntermediateFiles(coord.Tasks[i].id, nil)
		}

		if coord.Tasks[i].taskType == common.ReduceTask {
			response.IntermediateFiles = append([]common.IntermediateFileRef(nil), coord.Tasks[i].intermediateFiles...)
			for _, fileRef := range coord.Tasks[i].intermediateFiles {
				response.FileNames = append(response.FileNames, fileRef.FileName)
			}
		}

		if coord.Tasks[i].taskType == common.ReplicateTask {
			response.ReplicaFile = coord.Tasks[i].file
			response.ReplicaReduceID = coord.Tasks[i].reduceID
			response.ReplicaDestinationWorker = coord.Tasks[i].destinationWorker
			response.ReplicaDestinationAddr = coord.Tasks[i].destinationWorkerAddr
		}

		coord.Tasks[i].status = "working"
		if coord.currentPhase == common.MapTask {
			coord.MapTasks[coord.Tasks[i].id] = coord.Tasks[i]
		}
		if coord.currentPhase == common.ReduceTask {
			coord.ReduceTasks[coord.Tasks[i].id] = coord.Tasks[i]
		}

		return nil
	}

	return nil
}

func (coord *CoordinatorAPI) CompleteWork(request common.TaskDoneRequest, response *common.TaskDoneResponse) error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	coord.registerWorker(request.WorkerID, request.WorkerAddr)

	if request.TaskID < 0 || request.TaskID >= len(coord.Tasks) {
		response.Complete = false
		return fmt.Errorf("invalid task id %d", request.TaskID)
	}

	if !request.TaskSuccess {
		switch request.TaskType {
		case common.MapTask:
			coord.resetMapTask(request.TaskID, request.WorkerID)
		case common.ReduceTask:
			if len(request.MissingInputs) > 0 {
				coord.restoreMissingMapTasks(request.MissingInputs)
			} else {
				coord.resetReduceTask(request.TaskID, request.WorkerID)
			}
		case common.ReplicateTask:
			coord.resetReplicationTask(request.TaskID, request.WorkerID)
		}

		coord.updateCompletionState()
		response.Complete = false
		return nil
	}

	coord.Tasks[request.TaskID].status = "complete"
	coord.Tasks[request.TaskID].ownerWorker = request.WorkerID
	coord.Tasks[request.TaskID].outputFiles = append([]string(nil), request.OutputFiles...)

	if request.TaskType == common.MapTask {
		coord.Tasks[request.TaskID].intermediateFiles = coord.buildIntermediateFiles(request.TaskID, request.OutputFiles)
		coord.MapTasks[request.TaskID] = coord.Tasks[request.TaskID]
		coord.addDiscoveredUrls(request.DiscoveredUrls)
		coord.createMapTasksFromFrontier()
	}

	if request.TaskType == common.ReduceTask {
		coord.ReduceTasks[request.TaskID] = coord.Tasks[request.TaskID]
		coord.recordReduceOutput(request.TaskID, request.WorkerID, firstFileName(request.OutputFiles, request.TaskID))
	}

	if request.TaskType == common.ReplicateTask {
		coord.markReplicaStored(coord.Tasks[request.TaskID].reduceID, coord.Tasks[request.TaskID].destinationWorker)
	}

	coord.updateCompletionState()

	if coord.currentPhase == common.ReplicateTask && coord.TasksComplete {
		coord.appendMissingReplicationTasks()
		coord.updateCompletionState()
		if coord.TasksComplete && coord.replicationSatisfied() {
			coord.currentPhase = common.DoneTask
			response.Complete = true
			return nil
		}
	}

	response.Complete = coord.currentPhase == common.DoneTask
	return nil
}

func (coord *CoordinatorAPI) initializeReduceTasks() {
	coord.Tasks = nil
	coord.ReduceTasks = nil
	coord.ReduceOutputs = make(map[int]*ReduceOutputState)
	coord.currentPhase = common.ReduceTask

	for i := 0; i < coord.R; i++ {
		reduceTask := PendingTask{
			id:          i,
			taskType:    common.ReduceTask,
			status:      "idle",
			ownerWorker: coord.reduceOwner(i),
			reduceID:    i,
		}

		for mapTaskID := range len(coord.MapTasks) {
			for _, fileRef := range coord.MapTasks[mapTaskID].intermediateFiles {
				if fileRef.ReduceID == i {
					reduceTask.intermediateFiles = append(reduceTask.intermediateFiles, fileRef)
				}
			}
		}

		coord.Tasks = append(coord.Tasks, reduceTask)
		coord.ReduceTasks = append(coord.ReduceTasks, reduceTask)
	}

	coord.updateCompletionState()
}

func (coord *CoordinatorAPI) initializeReplicationTasks() {
	coord.Tasks = nil
	coord.currentPhase = common.ReplicateTask

	for reduceID := range coord.R {
		output := coord.ReduceOutputs[reduceID]
		if output == nil {
			continue
		}
		output.pendingReplicaTasks = make(map[int]bool)
	}

	coord.appendMissingReplicationTasks()
	coord.updateCompletionState()
}

func StartCoordinator(r int, numWorkers int, maxVisitedUrls int, desiredReplicaCount int, file *os.File) {
	if numWorkers < 1 {
		numWorkers = 1
	}

	coord := CoordinatorAPI{
		R:                   r,
		numWorkers:          numWorkers,
		MaxVisitedUrls:      maxVisitedUrls,
		DesiredReplicaCount: desiredReplicaCount,
		currentPhase:        common.MapTask,
		seenUrls:            make(map[string]bool),
		workerAddresses:     make(map[int]string),
		workerHostPrefix:    getEnvString("WORKER_HOST_PREFIX", "worker"),
		workerPortBase:      getEnvInt("WORKER_PORT_BASE", 9100),
		lastHeartBeat:       make(map[int]time.Time),
		failedWorkers:       make(map[int]bool),
		ReduceOutputs:       make(map[int]*ReduceOutputState),
	}

	for workerID := 1; workerID <= numWorkers; workerID++ {
		coord.lastHeartBeat[workerID] = time.Now()
		coord.workerAddresses[workerID] = coord.workerAddress(workerID)
	}

	if file != nil {
		fmt.Println("Splitting input file...")
		coord.initializeMapTasks(file)
		fmt.Println("Input file splitting completed.")
	}

	go coord.monitorWorkerHeartBeats()

	err := rpc.RegisterName("Coordinator", &coord)
	if err != nil {
		panic(fmt.Sprintf("Failed to register RPC service: %v", err))
	}

	err = rpc.RegisterName("CoordinatorAPI", &coord)
	if err != nil {
		panic(fmt.Sprintf("Failed to register CoordinatorAPI RPC service: %v", err))
	}

	listenAddr := os.Getenv("COORDINATOR_BIND_ADDR")
	if listenAddr == "" {
		listenAddr = os.Getenv("COORDINATOR_ADDR")
	}
	if listenAddr == "" {
		listenAddr = defaultCoordinatorAddr()
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on coordinator port: %v", err))
	}

	fmt.Printf("Server ready to delegate tasks to workers, listening on %s\n", listenAddr)

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Printf("Failed connection attempt %v", err)
			continue
		}
		fmt.Println("Accepted new worker connection...")

		go rpc.ServeConn(connection)
	}
}

func (coord *CoordinatorAPI) initializeMapTasks(file *os.File) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		url := strings.TrimSpace(scanner.Text())
		if url == "" || coord.seenUrls[url] {
			continue
		}

		coord.seenUrls[url] = true
		coord.frontier = append(coord.frontier, url)
	}

	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Error while reading seed URLs: %v", err))
	}

	coord.createMapTasksFromFrontier()
	coord.updateCompletionState()
}

func (coord *CoordinatorAPI) createMapTasksFromFrontier() {
	for len(coord.frontier) > 0 {
		remainingUrls := len(coord.frontier)
		if coord.MaxVisitedUrls > 0 {
			urlsLeftToVisit := coord.MaxVisitedUrls - coord.VisitedUrlCount
			if urlsLeftToVisit <= 0 {
				break
			}
			if remainingUrls > urlsLeftToVisit {
				remainingUrls = urlsLeftToVisit
			}
		}

		batchEnd := batchSize
		if remainingUrls < batchEnd {
			batchEnd = remainingUrls
		}

		task := PendingTask{
			id:          len(coord.MapTasks),
			batchUrls:   append([]string(nil), coord.frontier[:batchEnd]...),
			taskType:    common.MapTask,
			status:      "idle",
			ownerWorker: (len(coord.MapTasks) % coord.numWorkers) + 1,
		}

		coord.frontier = coord.frontier[batchEnd:]
		coord.VisitedUrlCount += batchEnd
		coord.MapTasks = append(coord.MapTasks, task)
		if coord.currentPhase == common.MapTask {
			coord.Tasks = append(coord.Tasks, task)
		}
	}
}

func (coord *CoordinatorAPI) addDiscoveredUrls(urls []string) {
	for _, rawUrl := range urls {
		url := strings.TrimSpace(rawUrl)
		if url == "" || coord.seenUrls[url] {
			continue
		}

		coord.seenUrls[url] = true
		coord.frontier = append(coord.frontier, url)
	}
}

func (coord *CoordinatorAPI) buildIntermediateFiles(taskID int, outputFiles []string) []common.IntermediateFileRef {
	intermediateFiles := make([]common.IntermediateFileRef, 0, coord.R)

	for reduceID := 0; reduceID < coord.R; reduceID++ {
		ownerWorker := coord.reduceOwner(reduceID)
		fileName := fmt.Sprintf("map-%03d-reduce-%03d.json", taskID, reduceID)
		if reduceID < len(outputFiles) && strings.TrimSpace(outputFiles[reduceID]) != "" {
			fileName = strings.TrimSpace(outputFiles[reduceID])
		}

		intermediateFiles = append(intermediateFiles, common.IntermediateFileRef{
			MapTaskID:  taskID,
			ReduceID:   reduceID,
			FileName:   fileName,
			WorkerID:   ownerWorker,
			WorkerAddr: coord.workerAddresses[ownerWorker],
		})
	}

	return intermediateFiles
}

func (coord *CoordinatorAPI) restoreMissingMapTasks(missingInputs []common.IntermediateFileRef) {
	coord.currentPhase = common.MapTask
	coord.Tasks = nil
	coord.ReduceTasks = nil
	coord.ReduceOutputs = make(map[int]*ReduceOutputState)

	missingMapTasks := make(map[int]bool)
	for _, input := range missingInputs {
		missingMapTasks[input.MapTaskID] = true
	}

	for mapTaskID := range missingMapTasks {
		if mapTaskID < 0 || mapTaskID >= len(coord.MapTasks) {
			continue
		}

		coord.MapTasks[mapTaskID].status = "idle"
		coord.MapTasks[mapTaskID].ownerWorker = coord.reassignWorker(mapTaskID, 0)
		coord.MapTasks[mapTaskID].outputFiles = nil
		coord.MapTasks[mapTaskID].intermediateFiles = nil
	}

	for i := range len(coord.MapTasks) {
		coord.Tasks = append(coord.Tasks, coord.MapTasks[i])
	}

	coord.updateCompletionState()
}

func (coord *CoordinatorAPI) restoreReduceTasks(reduceIDs []int) {
	if len(coord.ReduceTasks) == 0 {
		coord.initializeReduceTasks()
		return
	}

	missingReduceTasks := make(map[int]bool)
	for _, reduceID := range reduceIDs {
		missingReduceTasks[reduceID] = true
	}

	coord.currentPhase = common.ReduceTask
	coord.Tasks = nil

	for reduceID := range missingReduceTasks {
		if reduceID < 0 || reduceID >= len(coord.ReduceTasks) {
			continue
		}

		coord.ReduceTasks[reduceID].status = "idle"
		coord.ReduceTasks[reduceID].ownerWorker = coord.reassignWorker(reduceID, 0)
		coord.ReduceTasks[reduceID].outputFiles = nil
		delete(coord.ReduceOutputs, reduceID)
	}

	for i := range len(coord.ReduceTasks) {
		coord.Tasks = append(coord.Tasks, coord.ReduceTasks[i])
	}

	coord.updateCompletionState()
}

func (coord *CoordinatorAPI) resetMapTask(taskID int, failedWorker int) {
	if taskID < 0 || taskID >= len(coord.Tasks) {
		return
	}

	coord.Tasks[taskID].status = "idle"
	coord.Tasks[taskID].ownerWorker = coord.reassignWorker(taskID, failedWorker)
	coord.Tasks[taskID].outputFiles = nil
	coord.Tasks[taskID].intermediateFiles = nil
	coord.MapTasks[taskID] = coord.Tasks[taskID]
}

func (coord *CoordinatorAPI) resetReduceTask(taskID int, failedWorker int) {
	if taskID < 0 || taskID >= len(coord.Tasks) {
		return
	}

	coord.Tasks[taskID].status = "idle"
	coord.Tasks[taskID].ownerWorker = coord.reassignWorker(taskID, failedWorker)
	coord.Tasks[taskID].outputFiles = nil
	coord.ReduceTasks[taskID] = coord.Tasks[taskID]
	delete(coord.ReduceOutputs, taskID)
}

func (coord *CoordinatorAPI) resetReplicationTask(taskID int, failedWorker int) {
	if taskID < 0 || taskID >= len(coord.Tasks) {
		return
	}

	task := coord.Tasks[taskID]
	output := coord.ReduceOutputs[task.reduceID]
	if output != nil {
		delete(output.pendingReplicaTasks, task.destinationWorker)
	}

	if output == nil || len(output.holders) == 0 {
		coord.restoreReduceTasks([]int{task.reduceID})
		return
	}

	sourceWorker := coord.pickReplicaSourceWorker(output)
	destinationWorker := coord.pickReplicaDestinationWorker(output)
	if sourceWorker == 0 || destinationWorker == 0 {
		task.status = "complete"
		coord.Tasks[taskID] = task
		return
	}

	task.status = "idle"
	task.ownerWorker = sourceWorker
	task.destinationWorker = destinationWorker
	task.destinationWorkerAddr = coord.workerAddresses[destinationWorker]
	coord.Tasks[taskID] = task
	output.pendingReplicaTasks[destinationWorker] = true
}

func (coord *CoordinatorAPI) updateCompletionState() {
	coord.CompleteTaskCount = 0

	for i := range len(coord.Tasks) {
		if coord.Tasks[i].status == "complete" {
			coord.CompleteTaskCount += 1
		}
	}

	coord.TasksComplete = len(coord.Tasks) == 0 || coord.CompleteTaskCount == len(coord.Tasks)
}

func (coord *CoordinatorAPI) recordReduceOutput(reduceID int, workerID int, fileName string) {
	coord.ReduceOutputs[reduceID] = &ReduceOutputState{
		reduceID:            reduceID,
		fileName:            fileName,
		primaryWorker:       workerID,
		holders:             map[int]bool{workerID: true},
		pendingReplicaTasks: make(map[int]bool),
	}
}

func (coord *CoordinatorAPI) markReplicaStored(reduceID int, workerID int) {
	output := coord.ReduceOutputs[reduceID]
	if output == nil {
		return
	}

	delete(output.pendingReplicaTasks, workerID)
	output.holders[workerID] = true
}

func (coord *CoordinatorAPI) appendMissingReplicationTasks() {
	if coord.currentPhase != common.ReplicateTask {
		return
	}

	for reduceID := range len(coord.ReduceTasks) {
		output := coord.ReduceOutputs[reduceID]
		if output == nil {
			continue
		}

		if len(output.holders) == 0 {
			coord.restoreReduceTasks([]int{reduceID})
			return
		}

		for coord.outputCopyCount(output) < coord.requiredCopyCount() {
			sourceWorker := coord.pickReplicaSourceWorker(output)
			destinationWorker := coord.pickReplicaDestinationWorker(output)
			if sourceWorker == 0 || destinationWorker == 0 {
				break
			}

			task := PendingTask{
				id:                    len(coord.Tasks),
				file:                  output.fileName,
				taskType:              common.ReplicateTask,
				status:                "idle",
				ownerWorker:           sourceWorker,
				destinationWorker:     destinationWorker,
				destinationWorkerAddr: coord.workerAddresses[destinationWorker],
				reduceID:              reduceID,
			}

			coord.Tasks = append(coord.Tasks, task)
			output.pendingReplicaTasks[destinationWorker] = true
		}
	}
}

func (coord *CoordinatorAPI) replicationSatisfied() bool {
	if len(coord.ReduceTasks) == 0 {
		return true
	}

	for reduceID := range len(coord.ReduceTasks) {
		output := coord.ReduceOutputs[reduceID]
		if output == nil {
			return false
		}
		if len(output.holders) < coord.requiredCopyCount() {
			return false
		}
	}

	return true
}

func (coord *CoordinatorAPI) outputCopyCount(output *ReduceOutputState) int {
	return len(output.holders) + len(output.pendingReplicaTasks)
}

func (coord *CoordinatorAPI) requiredCopyCount() int {
	requiredCopies := coord.DesiredReplicaCount + 1
	activeWorkers := coord.activeWorkerCount()
	if requiredCopies > activeWorkers {
		return activeWorkers
	}
	if requiredCopies < 1 {
		return 1
	}
	return requiredCopies
}

func (coord *CoordinatorAPI) activeWorkerCount() int {
	activeWorkers := 0
	for workerID := 1; workerID <= coord.numWorkers; workerID++ {
		if coord.failedWorkers[workerID] {
			continue
		}
		activeWorkers += 1
	}

	if activeWorkers < 1 {
		return 1
	}

	return activeWorkers
}

func (coord *CoordinatorAPI) pickReplicaSourceWorker(output *ReduceOutputState) int {
	if output.holders[output.primaryWorker] && !coord.failedWorkers[output.primaryWorker] {
		return output.primaryWorker
	}

	for workerID := 1; workerID <= coord.numWorkers; workerID++ {
		if coord.failedWorkers[workerID] {
			continue
		}
		if output.holders[workerID] {
			return workerID
		}
	}

	return 0
}

func (coord *CoordinatorAPI) pickReplicaDestinationWorker(output *ReduceOutputState) int {
	bestWorkerID := 0
	bestUtilization := 0

	for workerID := 1; workerID <= coord.numWorkers; workerID++ {
		if coord.failedWorkers[workerID] {
			continue
		}
		if output.holders[workerID] || output.pendingReplicaTasks[workerID] {
			continue
		}

		utilization := coord.replicaUtilization(workerID)
		if bestWorkerID == 0 || utilization < bestUtilization || (utilization == bestUtilization && workerID < bestWorkerID) {
			bestWorkerID = workerID
			bestUtilization = utilization
		}
	}

	return bestWorkerID
}

func (coord *CoordinatorAPI) pickSearchWorker(output *ReduceOutputState) int {
	if output == nil {
		return 0
	}

	if output.holders[output.primaryWorker] && !coord.failedWorkers[output.primaryWorker] {
		return output.primaryWorker
	}

	for workerID := 1; workerID <= coord.numWorkers; workerID++ {
		if coord.failedWorkers[workerID] {
			continue
		}
		if output.holders[workerID] {
			return workerID
		}
	}

	return 0
}

func (coord *CoordinatorAPI) replicaUtilization(workerID int) int {
	replicaCount := 0

	for reduceID := range len(coord.ReduceTasks) {
		output := coord.ReduceOutputs[reduceID]
		if output == nil {
			continue
		}

		if workerID != output.primaryWorker && output.holders[workerID] {
			replicaCount += 1
		}

		if output.pendingReplicaTasks[workerID] {
			replicaCount += 1
		}
	}

	return replicaCount
}

func (coord *CoordinatorAPI) registerWorker(workerID int, workerAddr string) {
	if workerID <= 0 {
		return
	}

	if workerAddr != "" {
		coord.workerAddresses[workerID] = workerAddr
	}

	coord.lastHeartBeat[workerID] = time.Now()
	coord.failedWorkers[workerID] = false
}

func (coord *CoordinatorAPI) reassignWorker(taskID int, failedWorker int) int {
	for offset := 0; offset < coord.numWorkers; offset++ {
		workerID := ((taskID + offset) % coord.numWorkers) + 1
		if workerID == failedWorker {
			continue
		}
		if coord.failedWorkers[workerID] {
			continue
		}
		return workerID
	}

	return ((taskID % coord.numWorkers) + 1)
}

func (coord *CoordinatorAPI) monitorWorkerHeartBeats() {
	ticker := time.NewTicker(heartbeatCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		coord.mu.Lock()
		coord.reassignTimedOutWorkers()
		coord.mu.Unlock()
	}
}

func (coord *CoordinatorAPI) reassignTimedOutWorkers() {
	now := time.Now()
	for workerID, lastHeartBeat := range coord.lastHeartBeat {
		if coord.failedWorkers[workerID] {
			continue
		}

		if now.Sub(lastHeartBeat) < heartbeatTimeout {
			continue
		}

		coord.failedWorkers[workerID] = true
		log.Printf("Worker %d heartbeat timed out, reassigning tasks", workerID)
		coord.reassignWorkerTasks(workerID)
	}
}

func (coord *CoordinatorAPI) reassignWorkerTasks(workerID int) {
	switch coord.currentPhase {
	case common.MapTask:
		coord.resetOwnedMapTasks(workerID)
		lostInputs := coord.findLostIntermediateFiles(workerID)
		if len(lostInputs) > 0 {
			coord.restoreMissingMapTasks(lostInputs)
		} else {
			coord.updateCompletionState()
		}
	case common.ReduceTask:
		lostInputs := coord.findLostIntermediateFiles(workerID)
		if len(lostInputs) > 0 {
			coord.restoreMissingMapTasks(lostInputs)
			return
		}

		lostReduceIDs := coord.removeWorkerFromOutputs(workerID)
		if len(lostReduceIDs) > 0 {
			coord.restoreReduceTasks(lostReduceIDs)
			return
		}

		coord.resetOwnedReduceTasks(workerID)
		coord.updateCompletionState()
	case common.ReplicateTask:
		lostReduceIDs := coord.removeWorkerFromOutputs(workerID)
		if len(lostReduceIDs) > 0 {
			coord.restoreReduceTasks(lostReduceIDs)
			return
		}

		coord.reconfigureReplicationTasks(workerID)
		coord.appendMissingReplicationTasks()
		coord.updateCompletionState()
	}
}

func (coord *CoordinatorAPI) resetOwnedMapTasks(workerID int) {
	for mapTaskID := range len(coord.MapTasks) {
		if coord.MapTasks[mapTaskID].ownerWorker != workerID {
			continue
		}
		if coord.MapTasks[mapTaskID].status == "complete" {
			continue
		}

		coord.MapTasks[mapTaskID].status = "idle"
		coord.MapTasks[mapTaskID].ownerWorker = coord.reassignWorker(mapTaskID, workerID)
		coord.MapTasks[mapTaskID].outputFiles = nil
		coord.MapTasks[mapTaskID].intermediateFiles = nil
		if mapTaskID < len(coord.Tasks) {
			coord.Tasks[mapTaskID] = coord.MapTasks[mapTaskID]
		}
	}
}

func (coord *CoordinatorAPI) resetOwnedReduceTasks(workerID int) {
	for reduceID := range len(coord.ReduceTasks) {
		if coord.ReduceTasks[reduceID].ownerWorker != workerID {
			continue
		}
		if coord.ReduceTasks[reduceID].status == "complete" {
			continue
		}

		coord.ReduceTasks[reduceID].status = "idle"
		coord.ReduceTasks[reduceID].ownerWorker = coord.reassignWorker(reduceID, workerID)
		coord.ReduceTasks[reduceID].outputFiles = nil
		if reduceID < len(coord.Tasks) {
			coord.Tasks[reduceID] = coord.ReduceTasks[reduceID]
		}
	}
}

func (coord *CoordinatorAPI) findLostIntermediateFiles(workerID int) []common.IntermediateFileRef {
	lostInputs := make([]common.IntermediateFileRef, 0)
	seenMapTasks := make(map[int]bool)

	for mapTaskID := range len(coord.MapTasks) {
		if coord.MapTasks[mapTaskID].status != "complete" {
			continue
		}

		for _, fileRef := range coord.MapTasks[mapTaskID].intermediateFiles {
			if fileRef.WorkerID != workerID {
				continue
			}
			if seenMapTasks[fileRef.MapTaskID] {
				continue
			}

			seenMapTasks[fileRef.MapTaskID] = true
			lostInputs = append(lostInputs, fileRef)
		}
	}

	return lostInputs
}

func (coord *CoordinatorAPI) removeWorkerFromOutputs(workerID int) []int {
	lostReduceIDs := make([]int, 0)

	for reduceID := range len(coord.ReduceTasks) {
		output := coord.ReduceOutputs[reduceID]
		if output == nil {
			continue
		}

		delete(output.holders, workerID)
		delete(output.pendingReplicaTasks, workerID)

		if len(output.holders) == 0 {
			lostReduceIDs = append(lostReduceIDs, reduceID)
		}
	}

	return lostReduceIDs
}

func (coord *CoordinatorAPI) reconfigureReplicationTasks(workerID int) {
	for taskID := range len(coord.Tasks) {
		if coord.Tasks[taskID].taskType != common.ReplicateTask {
			continue
		}
		if coord.Tasks[taskID].status == "complete" {
			continue
		}
		if coord.Tasks[taskID].ownerWorker != workerID && coord.Tasks[taskID].destinationWorker != workerID {
			continue
		}

		output := coord.ReduceOutputs[coord.Tasks[taskID].reduceID]
		if output == nil {
			continue
		}

		delete(output.pendingReplicaTasks, coord.Tasks[taskID].destinationWorker)

		sourceWorker := coord.pickReplicaSourceWorker(output)
		destinationWorker := coord.pickReplicaDestinationWorker(output)
		if sourceWorker == 0 || destinationWorker == 0 {
			coord.Tasks[taskID].status = "complete"
			continue
		}

		coord.Tasks[taskID].status = "idle"
		coord.Tasks[taskID].ownerWorker = sourceWorker
		coord.Tasks[taskID].destinationWorker = destinationWorker
		coord.Tasks[taskID].destinationWorkerAddr = coord.workerAddresses[destinationWorker]
		output.pendingReplicaTasks[destinationWorker] = true
	}
}

func defaultCoordinatorAddr() string {
	return ":9000"
}

func (coord *CoordinatorAPI) reduceOwner(reduceID int) int {
	return (reduceID % coord.numWorkers) + 1
}

func (coord *CoordinatorAPI) workerAddress(workerID int) string {
	return fmt.Sprintf("%s%d:%d", coord.workerHostPrefix, workerID, coord.workerPortBase+workerID)
}

func getEnvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func getEnvString(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	return value
}

func firstFileName(outputFiles []string, reduceID int) string {
	if len(outputFiles) > 0 && strings.TrimSpace(outputFiles[0]) != "" {
		return filepath.Base(strings.TrimSpace(outputFiles[0]))
	}

	return fmt.Sprintf("reduce-%03d.json", reduceID)
}

func partitionWord(word string, numReduce int) int {
	if numReduce <= 1 {
		return 0
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(word))
	return int(hasher.Sum32() % uint32(numReduce))
}
