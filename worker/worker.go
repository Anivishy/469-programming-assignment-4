package worker

import (
    "encoding/json"
    "fmt"
    "hash/fnv"
    "html"
    "io"
    "log"
    "net"
    "net/http"
    "net/rpc"
    "net/url"
    "os"
    "path/filepath"
    "regexp"
    "sort"
    "strings"
    "sync"
    "time"

    "distributed_search_engine/common"
)

const (
    taskPollInterval = 2 * time.Second
    heartbeatInterval = 10 * time.Second
    rpcDialTimeout = 5 * time.Second
    rpcRetryDelay = 2 * time.Second
    pageFetchTimeout = 10 * time.Second
    maxPageBytes = 2 * 1024 * 1024
)

var hrefPattern = regexp.MustCompile(`(?i)href\s*=\s*["']([^"'#]+)["']`)
var tagPattern = regexp.MustCompile(`(?s)<[^>]*>`)
var wordPattern = regexp.MustCompile(`[A-Za-z0-9]+`)

type Worker struct {
    mu sync.Mutex

    id int
    bindAddr string
    advertiseAddr string
    coordinatorAddr string
    dataDir string
    currentTaskType common.TaskType
    currentTaskID int

    httpClient *http.Client
    done chan struct{}
    once sync.Once
}

func StartWorker(workerID int, bindAddr, advertiseAddr, coordinatorAddr, dataDir string) {
    worker := &Worker{
        id: workerID,
        bindAddr: bindAddr,
        advertiseAddr: advertiseAddr,
        coordinatorAddr: coordinatorAddr,
        dataDir: dataDir,
        httpClient: &http.Client{Timeout: pageFetchTimeout},
        done: make(chan struct{}),
    }

    if err := worker.run(); err != nil {
        panic(err)
    }
}

func (w *Worker) run() error {
    if err := os.MkdirAll(w.intermediateDir(), 0o755); err != nil {
        return err
    }

    if err := os.MkdirAll(w.reduceDir(), 0o755); err != nil {
        return err
    }

    if err := os.MkdirAll(w.replicaDir(), 0o755); err != nil {
        return err
    }

    server := rpc.NewServer()
    if err := server.RegisterName("Worker", w); err != nil {
        return err
    }

    listener, err := net.Listen("tcp", w.bindAddr)
    if err != nil {
        return err
    }
    defer listener.Close()

    log.Printf(
        "worker %d listening on %s advertising %s coordinator=%s dataDir=%s",
        w.id,
        w.bindAddr,
        w.advertiseAddr,
        w.coordinatorAddr,
        w.dataDir,
    )

    go w.acceptLoop(server, listener)
    go w.heartbeatLoop()

    return w.workLoop()
}

func (w *Worker) acceptLoop(server *rpc.Server, listener net.Listener) {
    for {
        conn, err := listener.Accept()
        if err != nil {
            select {
            case <-w.done:
                return
            default:
                log.Printf("worker %d accept error: %v", w.id, err)
                continue
            }
        }

        go server.ServeConn(conn)
    }
}

func (w *Worker) heartbeatLoop() {
    w.sendHeartbeat()

    ticker := time.NewTicker(heartbeatInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            w.sendHeartbeat()
        case <-w.done:
            return
        }
    }
}

func (w *Worker) sendHeartbeat() {
    currentTaskType, currentTaskID := w.currentTaskSnapshot()

    request := common.HeartBeatRequest{
        WorkerID: w.id,
        WorkerAddr: w.advertiseAddr,
        CurrentTaskType: currentTaskType,
        CurrentTaskID: currentTaskID,
    }

    var response common.HeartBeatResponse
    if err := callRPC(w.coordinatorAddr, "Coordinator.HeartBeat", request, &response); err != nil {
        log.Printf("worker %d heartbeat failed: %v", w.id, err)
        return
    }

    if response.CoordinatorDone {
        w.markDone()
    }
}

func (w *Worker) workLoop() error {
    for {
        select {
        case <-w.done:
            return nil
        default:
        }

        task, err := w.requestTask()
        if err != nil {
            log.Printf("worker %d failed to request task: %v", w.id, err)
            if w.sleepOrDone(rpcRetryDelay) {
                return nil
            }
            continue
        }

        switch task.TaskType {
        case common.MapTask:
            if err := w.processTask(task); err != nil {
                log.Printf("worker %d map task %d handling error: %v", w.id, task.TaskID, err)
                if w.sleepOrDone(rpcRetryDelay) {
                    return nil
                }
            }
        case common.ReduceTask:
            if err := w.processTask(task); err != nil {
                log.Printf("worker %d reduce task %d handling error: %v", w.id, task.TaskID, err)
                if w.sleepOrDone(rpcRetryDelay) {
                    return nil
                }
            }
        case common.ReplicateTask:
            if err := w.processTask(task); err != nil {
                log.Printf("worker %d replicate task %d handling error: %v", w.id, task.TaskID, err)
                if w.sleepOrDone(rpcRetryDelay) {
                    return nil
                }
            }
        case common.WaitTask:
            if w.sleepOrDone(taskPollInterval) {
                return nil
            }
        case common.DoneTask:
            w.markDone()
            return nil
        default:
            log.Printf("worker %d received unknown task type %q", w.id, task.TaskType)
            if w.sleepOrDone(taskPollInterval) {
                return nil
            }
        }
    }
}

func (w *Worker) requestTask() (common.TaskResponse, error) {
    request := common.TaskRequest{
        WorkerID: w.id,
        WorkerAddr: w.advertiseAddr,
    }

    var response common.TaskResponse
    err := callRPC(w.coordinatorAddr, "Coordinator.RequestTask", request, &response)
    return response, err
}

func (w *Worker) processTask(task common.TaskResponse) error {
    w.setCurrentTask(task.TaskType, task.TaskID)

    completion := w.executeTask(task)
    for {
        var response common.TaskDoneResponse
        err := callRPC(w.coordinatorAddr, "Coordinator.TaskDone", completion, &response)
        if err != nil {
            log.Printf("worker %d failed to report task %d completion: %v", w.id, task.TaskID, err)
            if w.sleepOrDone(rpcRetryDelay) {
                return nil
            }
            continue
        }

        w.clearCurrentTask()
        if response.Complete {
            w.markDone()
        }
        return nil
    }
}

func (w *Worker) executeTask(task common.TaskResponse) common.TaskDoneRequest {
    switch task.TaskType {
    case common.MapTask:
        return w.executeMapTask(task)
    case common.ReduceTask:
        return w.executeReduceTask(task)
    case common.ReplicateTask:
        return w.executeReplicateTask(task)
    default:
        return common.TaskDoneRequest{
            WorkerID: w.id,
            WorkerAddr: w.advertiseAddr,
            TaskType: task.TaskType,
            TaskID: task.TaskID,
            TaskSuccess: false,
            ErrorMessage: "unsupported task type",
        }
    }
}

func (w *Worker) executeMapTask(task common.TaskResponse) common.TaskDoneRequest {
    completion := common.TaskDoneRequest{
        WorkerID: w.id,
        WorkerAddr: w.advertiseAddr,
        TaskType: common.MapTask,
        TaskID: task.TaskID,
    }

    if task.NumReduce <= 0 {
        completion.TaskSuccess = true
        return completion
    }

    shards := make([]map[string]map[string]struct{}, task.NumReduce)
    for reduceID := 0; reduceID < task.NumReduce; reduceID++ {
        shards[reduceID] = make(map[string]map[string]struct{})
    }

    discoveredURLs := make(map[string]struct{})
    for _, rawURL := range task.BatchUrls {
        words, links, err := w.fetchPageData(rawURL)
        if err != nil {
            log.Printf("worker %d failed to fetch %s: %v", w.id, rawURL, err)
            continue
        }

        for _, word := range words {
            reduceID := partitionWord(word, task.NumReduce)
            if _, ok := shards[reduceID][word]; !ok {
                shards[reduceID][word] = make(map[string]struct{})
            }
            shards[reduceID][word][rawURL] = struct{}{}
        }

        for _, link := range links {
            discoveredURLs[link] = struct{}{}
        }
    }

    outputFiles, err := w.writeMapOutputs(task.TaskID, shards)
    if err != nil {
        completion.TaskSuccess = false
        completion.ErrorMessage = err.Error()
        return completion
    }

    if err := w.pushIntermediateFiles(task, outputFiles); err != nil {
        completion.TaskSuccess = false
        completion.ErrorMessage = err.Error()
        return completion
    }

    completion.DiscoveredUrls = sortedStringSet(discoveredURLs)
    completion.OutputFiles = outputFiles
    completion.TaskSuccess = true
    return completion
}

func (w *Worker) executeReduceTask(task common.TaskResponse) common.TaskDoneRequest {
    completion := common.TaskDoneRequest{
        WorkerID: w.id,
        WorkerAddr: w.advertiseAddr,
        TaskType: common.ReduceTask,
        TaskID: task.TaskID,
    }

    combined := make(map[string]map[string]struct{})
    missingInputs := make([]common.IntermediateFileRef, 0)

    for _, input := range task.IntermediateFiles {
        payload, err := w.readIntermediateInput(input.FileName)
        if err != nil {
            missingInputs = append(missingInputs, input)
            log.Printf(
                "worker %d missing intermediate input map=%d reduce=%d file=%s: %v",
                w.id,
                input.MapTaskID,
                input.ReduceID,
                input.FileName,
                err,
            )
            continue
        }

        for word, urls := range payload {
            if _, ok := combined[word]; !ok {
                combined[word] = make(map[string]struct{})
            }

            for _, sourceURL := range urls {
                combined[word][sourceURL] = struct{}{}
            }
        }
    }

    if len(missingInputs) > 0 {
        completion.TaskSuccess = false
        completion.ErrorMessage = "missing intermediate inputs"
        completion.MissingInputs = missingInputs
        return completion
    }

    outputFile, err := w.writeReduceOutput(task.TaskID, combined)
    if err != nil {
        completion.TaskSuccess = false
        completion.ErrorMessage = err.Error()
        return completion
    }

    completion.OutputFiles = []string{outputFile}
    completion.TaskSuccess = true
    return completion
}

func (w *Worker) executeReplicateTask(task common.TaskResponse) common.TaskDoneRequest {
    completion := common.TaskDoneRequest{
        WorkerID: w.id,
        WorkerAddr: w.advertiseAddr,
        TaskType: common.ReplicateTask,
        TaskID: task.TaskID,
    }

    if task.ReplicaFile == "" {
        completion.TaskSuccess = false
        completion.ErrorMessage = "missing replica source file"
        return completion
    }

    if task.ReplicaDestinationAddr == "" {
        completion.TaskSuccess = false
        completion.ErrorMessage = "missing replica destination address"
        return completion
    }

    if err := w.transferReplicaFile(task.ReplicaReduceID, task.ReplicaFile, task.ReplicaDestinationAddr); err != nil {
        completion.TaskSuccess = false
        completion.ErrorMessage = err.Error()
        return completion
    }

    completion.OutputFiles = []string{task.ReplicaFile}
    completion.TaskSuccess = true
    return completion
}

func (w *Worker) fetchPageData(rawURL string) ([]string, []string, error) {
    response, err := w.httpClient.Get(rawURL)
    if err != nil {
        return nil, nil, err
    }
    defer response.Body.Close()

    body, err := io.ReadAll(io.LimitReader(response.Body, maxPageBytes))
    if err != nil {
        return nil, nil, err
    }

    bodyText := string(body)
    words := extractWords(bodyText)
    links := extractLinks(bodyText, rawURL)
    return words, links, nil
}

func (w *Worker) writeMapOutputs(taskID int, shards []map[string]map[string]struct{}) ([]string, error) {
    outputFiles := make([]string, len(shards))

    for reduceID, shard := range shards {
        output := make(map[string][]string, len(shard))
        for word, urls := range shard {
            output[word] = sortedStringSet(urls)
        }

        fileName := fmt.Sprintf("map-%03d-reduce-%03d.json", taskID, reduceID)
        filePath := filepath.Join(w.intermediateDir(), fileName)
        if err := writeJSONFile(filePath, output); err != nil {
            return nil, err
        }

        outputFiles[reduceID] = fileName
    }

    return outputFiles, nil
}

func (w *Worker) writeReduceOutput(taskID int, combined map[string]map[string]struct{}) (string, error) {
    output := make(map[string][]string, len(combined))
    for word, urls := range combined {
        output[word] = sortedStringSet(urls)
    }

    fileName := fmt.Sprintf("reduce-%03d.json", taskID)
    filePath := filepath.Join(w.reduceDir(), fileName)
    if err := writeJSONFile(filePath, output); err != nil {
        return "", err
    }

    return fileName, nil
}

func (w *Worker) pushIntermediateFiles(task common.TaskResponse, outputFiles []string) error {
    for reduceID, fileName := range outputFiles {
        if reduceID >= len(task.IntermediateFiles) {
            return fmt.Errorf("missing reduce destination metadata for partition %d", reduceID)
        }

        destination := task.IntermediateFiles[reduceID]
        if destination.WorkerID == w.id {
            continue
        }

        if destination.WorkerAddr == "" {
            return fmt.Errorf("missing destination address for reduce partition %d", reduceID)
        }

        if err := w.transferIntermediateFile(task.TaskID, reduceID, fileName, destination.WorkerAddr); err != nil {
            return err
        }
    }

    return nil
}

func (w *Worker) readIntermediateInput(fileName string) (map[string][]string, error) {
    data, err := os.ReadFile(filepath.Join(w.intermediateDir(), filepath.Base(fileName)))
    if err != nil {
        return nil, err
    }

    payload := make(map[string][]string)
    if len(data) == 0 {
        return payload, nil
    }

    if err := json.Unmarshal(data, &payload); err != nil {
        return nil, err
    }

    return payload, nil
}

func (w *Worker) transferIntermediateFile(mapTaskID int, reduceID int, fileName string, destinationAddr string) error {
    data, err := os.ReadFile(filepath.Join(w.intermediateDir(), filepath.Base(fileName)))
    if err != nil {
        return err
    }

    request := common.IntermediateTransferRequest{
        FromWorkerID: w.id,
        MapTaskID: mapTaskID,
        ReduceID: reduceID,
        Data: string(data),
    }
    var response common.IntermediateTransferResponse

    return callRPC(destinationAddr, "Worker.StoreIntermediate", request, &response)
}

func (w *Worker) transferReplicaFile(reduceID int, fileName string, destinationAddr string) error {
    data, err := w.readReduceOrReplicaFile(fileName)
    if err != nil {
        return err
    }

    request := common.ReplicaTransferRequest{
        FromWorkerID: w.id,
        ReduceID: reduceID,
        FileName: fileName,
        Data: string(data),
    }
    var response common.ReplicaTransferResponse

    return callRPC(destinationAddr, "Worker.StoreReplica", request, &response)
}

func (w *Worker) StoreIntermediate(request common.IntermediateTransferRequest, response *common.IntermediateTransferResponse) error {
    fileName := fmt.Sprintf("map-%03d-reduce-%03d.json", request.MapTaskID, request.ReduceID)
    filePath := filepath.Join(w.intermediateDir(), fileName)
    if err := os.WriteFile(filePath, []byte(request.Data), 0o644); err != nil {
        return err
    }

    response.Stored = true
    return nil
}

func (w *Worker) StoreReplica(request common.ReplicaTransferRequest, response *common.ReplicaTransferResponse) error {
    filePath := filepath.Join(w.replicaDir(), filepath.Base(request.FileName))
    if err := os.WriteFile(filePath, []byte(request.Data), 0o644); err != nil {
        return err
    }

    response.Stored = true
    return nil
}

func (w *Worker) setCurrentTask(taskType common.TaskType, taskID int) {
    w.mu.Lock()
    defer w.mu.Unlock()

    w.currentTaskType = taskType
    w.currentTaskID = taskID
}

func (w *Worker) clearCurrentTask() {
    w.mu.Lock()
    defer w.mu.Unlock()

    w.currentTaskType = ""
    w.currentTaskID = 0
}

func (w *Worker) currentTaskSnapshot() (common.TaskType, int) {
    w.mu.Lock()
    defer w.mu.Unlock()

    return w.currentTaskType, w.currentTaskID
}

func (w *Worker) markDone() {
    w.once.Do(func() {
        close(w.done)
    })
}

func (w *Worker) sleepOrDone(duration time.Duration) bool {
    timer := time.NewTimer(duration)
    defer timer.Stop()

    select {
    case <-timer.C:
        return false
    case <-w.done:
        return true
    }
}

func (w *Worker) intermediateDir() string {
    return filepath.Join(w.dataDir, "intermediate")
}

func (w *Worker) reduceDir() string {
    return filepath.Join(w.dataDir, "reduce")
}

func (w *Worker) replicaDir() string {
    return filepath.Join(w.dataDir, "replica")
}

func partitionWord(word string, numReduce int) int {
    if numReduce <= 1 {
        return 0
    }

    hasher := fnv.New32a()
    _, _ = hasher.Write([]byte(word))
    return int(hasher.Sum32() % uint32(numReduce))
}

func extractWords(body string) []string {
    plainText := tagPattern.ReplaceAllString(body, " ")
    plainText = html.UnescapeString(plainText)
    rawWords := wordPattern.FindAllString(strings.ToLower(plainText), -1)

    words := make([]string, 0, len(rawWords))
    for _, word := range rawWords {
        normalized := strings.TrimSpace(word)
        if normalized == "" {
            continue
        }
        words = append(words, normalized)
    }

    return words
}

func extractLinks(body, rawBaseURL string) []string {
    baseURL, err := url.Parse(rawBaseURL)
    if err != nil {
        return nil
    }

    links := make(map[string]struct{})
    for _, match := range hrefPattern.FindAllStringSubmatch(body, -1) {
        if len(match) < 2 {
            continue
        }

        target, err := url.Parse(strings.TrimSpace(match[1]))
        if err != nil {
            continue
        }

        resolved := baseURL.ResolveReference(target)
        if resolved.Scheme != "http" && resolved.Scheme != "https" {
            continue
        }

        resolved.Fragment = ""
        links[resolved.String()] = struct{}{}
    }

    return sortedStringSet(links)
}

func sortedStringSet(values map[string]struct{}) []string {
    result := make([]string, 0, len(values))
    for value := range values {
        result = append(result, value)
    }

    sort.Strings(result)
    return result
}

func writeJSONFile(filePath string, value any) error {
    if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
        return err
    }

    data, err := json.MarshalIndent(value, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(filePath, data, 0o644)
}

func (w *Worker) readReduceOrReplicaFile(fileName string) ([]byte, error) {
    reducePath := filepath.Join(w.reduceDir(), filepath.Base(fileName))
    if data, err := os.ReadFile(reducePath); err == nil {
        return data, nil
    }

    replicaPath := filepath.Join(w.replicaDir(), filepath.Base(fileName))
    return os.ReadFile(replicaPath)
}

func callRPC(address, method string, request any, response any) error {
    conn, err := net.DialTimeout("tcp", address, rpcDialTimeout)
    if err != nil {
        return err
    }

    client := rpc.NewClient(conn)
    defer client.Close()

    return client.Call(method, request, response)
}
