package coordinator

import (
    "bufio"
    "fmt"
    "log"
    "net"
    "net/rpc"
    "os"
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
    id int
    batchUrls []string
    intermediateFiles []common.IntermediateFileRef
    outputFiles []string
    taskType common.TaskType
    status string
    ownerWorker int
}

type CoordinatorAPI struct {
    Tasks []PendingTask
    MapTasks []PendingTask
    R int
    numWorkers int
    MaxVisitedUrls int
    VisitedUrlCount int
    CompleteTaskCount int
    TasksComplete bool
    currentPhase common.TaskType
    frontier []string
    seenUrls map[string]bool
    workerAddresses map[int]string
    workerHostPrefix string
    workerPortBase int
    lastHeartBeat map[int]time.Time
    failedWorkers map[int]bool
    mu sync.Mutex
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
    response.CoordinatorDone = coord.TasksComplete && coord.currentPhase == common.ReduceTask
    return nil
}

func (coord *CoordinatorAPI) GetWork(request common.TaskRequest, response *common.TaskResponse) error {
    coord.mu.Lock()
    defer coord.mu.Unlock()

    coord.registerWorker(request.WorkerID, request.WorkerAddr)
    coord.reassignTimedOutWorkers()

    *response = common.TaskResponse{
        TaskType: common.WaitTask,
        NumMap: len(coord.MapTasks),
        NumReduce: coord.R,
    }

    if coord.TasksComplete {
        if coord.currentPhase == common.MapTask {
            coord.initializeReduceTasks()
        } else {
            response.TaskType = common.DoneTask
            return nil
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

        coord.Tasks[i].status = "working"
        if coord.currentPhase == common.MapTask {
            coord.MapTasks[coord.Tasks[i].id] = coord.Tasks[i]
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
        coord.resetTask(request.TaskID, request.WorkerID)
        if request.TaskType == common.ReduceTask && len(request.MissingInputs) > 0 {
            coord.restoreMissingMapTasks(request.MissingInputs)
        }
        response.Complete = false
        return nil
    }

    if coord.Tasks[request.TaskID].status != "complete" {
        coord.CompleteTaskCount += 1
        coord.Tasks[request.TaskID].status = "complete"
    }

    coord.Tasks[request.TaskID].ownerWorker = request.WorkerID
    coord.Tasks[request.TaskID].outputFiles = append([]string(nil), request.OutputFiles...)

    if request.TaskType == common.MapTask {
        coord.Tasks[request.TaskID].intermediateFiles = coord.buildIntermediateFiles(request.TaskID, request.OutputFiles)
        coord.MapTasks[request.TaskID] = coord.Tasks[request.TaskID]
        coord.addDiscoveredUrls(request.DiscoveredUrls)
        coord.createMapTasksFromFrontier()
    }

    coord.updateCompletionState()
    response.Complete = coord.TasksComplete && coord.currentPhase == common.ReduceTask
    return nil
}

func (coord *CoordinatorAPI) initializeReduceTasks() {
    coord.Tasks = nil
    coord.CompleteTaskCount = 0
    coord.TasksComplete = false
    coord.currentPhase = common.ReduceTask

    for i := 0; i < coord.R; i++ {
        reduceTask := PendingTask{
            id: i,
            taskType: common.ReduceTask,
            status: "idle",
            ownerWorker: (i % coord.numWorkers) + 1,
        }

        for mapTaskID := range len(coord.MapTasks) {
            for _, fileRef := range coord.MapTasks[mapTaskID].intermediateFiles {
                if fileRef.ReduceID == i {
                    reduceTask.intermediateFiles = append(reduceTask.intermediateFiles, fileRef)
                }
            }
        }

        coord.Tasks = append(coord.Tasks, reduceTask)
    }

    if len(coord.Tasks) == 0 {
        coord.TasksComplete = true
    }
}

func StartCoordinator(r int, numWorkers int, maxVisitedUrls int, file *os.File) {
    if numWorkers < 1 {
        numWorkers = 1
    }

    coord := CoordinatorAPI{
        R: r,
        numWorkers: numWorkers,
        MaxVisitedUrls: maxVisitedUrls,
        currentPhase: common.MapTask,
        seenUrls: make(map[string]bool),
        workerAddresses: make(map[int]string),
        workerHostPrefix: getEnvString("WORKER_HOST_PREFIX", "worker"),
        workerPortBase: getEnvInt("WORKER_PORT_BASE", 9100),
        lastHeartBeat: make(map[int]time.Time),
        failedWorkers: make(map[int]bool),
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
            id: len(coord.MapTasks),
            batchUrls: append([]string(nil), coord.frontier[:batchEnd]...),
            taskType: common.MapTask,
            status: "idle",
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
            MapTaskID: taskID,
            ReduceID: reduceID,
            FileName: fileName,
            WorkerID: ownerWorker,
            WorkerAddr: coord.workerAddresses[ownerWorker],
        })
    }

    return intermediateFiles
}

func (coord *CoordinatorAPI) restoreMissingMapTasks(missingInputs []common.IntermediateFileRef) {
    coord.currentPhase = common.MapTask
    coord.Tasks = nil
    coord.CompleteTaskCount = 0
    coord.TasksComplete = false

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
        if coord.MapTasks[i].status == "complete" {
            coord.CompleteTaskCount += 1
        }
    }
}

func (coord *CoordinatorAPI) resetTask(taskID int, failedWorker int) {
    if taskID < 0 || taskID >= len(coord.Tasks) {
        return
    }

    if coord.Tasks[taskID].status == "complete" && coord.CompleteTaskCount > 0 {
        coord.CompleteTaskCount -= 1
    }

    coord.Tasks[taskID].status = "idle"
    coord.Tasks[taskID].ownerWorker = coord.reassignWorker(taskID, failedWorker)
    coord.Tasks[taskID].outputFiles = nil
    if coord.Tasks[taskID].taskType == common.MapTask {
        coord.Tasks[taskID].intermediateFiles = nil
        coord.MapTasks[taskID] = coord.Tasks[taskID]
    }

    coord.TasksComplete = false
}

func (coord *CoordinatorAPI) updateCompletionState() {
    coord.TasksComplete = false

    if len(coord.Tasks) == 0 {
        coord.TasksComplete = true
        return
    }

    if coord.CompleteTaskCount == len(coord.Tasks) {
        coord.TasksComplete = true
    }
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
    if coord.currentPhase == common.MapTask {
        for i := range len(coord.Tasks) {
            if coord.Tasks[i].ownerWorker != workerID {
                continue
            }
            coord.resetTask(i, workerID)
        }
        return
    }

    missingInputs := make([]common.IntermediateFileRef, 0)
    for i := range len(coord.MapTasks) {
        for _, fileRef := range coord.MapTasks[i].intermediateFiles {
            if fileRef.WorkerID != workerID {
                continue
            }
            missingInputs = append(missingInputs, fileRef)
        }
    }

    if len(missingInputs) > 0 {
        coord.restoreMissingMapTasks(missingInputs)
        return
    }

    for i := range len(coord.Tasks) {
        if coord.Tasks[i].ownerWorker != workerID {
            continue
        }
        coord.resetTask(i, workerID)
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
