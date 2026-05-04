package client

import (
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"distributed_search_engine/common"
)

const rpcDialTimeout = 5 * time.Second

func StartClient(clientID int, coordinatorAddr string, threads int, keyword string) {
	if threads < 1 {
		threads = 1
	}

	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		keyword = "the"
	}

	log.Printf("client %d ready coordinator=%s threads=%d keyword=%s", clientID, coordinatorAddr, threads, keyword)

	var wg sync.WaitGroup
	for threadID := 1; threadID <= threads; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			request := common.SearchRequest{
				ClientID: clientID,
				ThreadID: threadID,
				Keyword:  keyword,
			}

			var response common.SearchResponse
			if err := callRPC(coordinatorAddr, "Coordinator.Search", request, &response); err != nil {
				log.Printf("client %d thread %d search request failed: %v", clientID, threadID, err)
				return
			}

			if !response.Ready || response.WorkerAddr == "" || response.FileName == "" {
				log.Printf("client %d thread %d search not ready yet", clientID, threadID)
				return
			}

			workerRequest := common.WorkerSearchRequest{
				Keyword:  keyword,
				FileName: response.FileName,
			}
			var workerResponse common.WorkerSearchResponse
			if err := callRPC(response.WorkerAddr, "Worker.Search", workerRequest, &workerResponse); err != nil {
				log.Printf("client %d thread %d worker search failed: %v", clientID, threadID, err)
				return
			}

			urlsJSON, err := json.Marshal(workerResponse.URLs)
			if err != nil {
				log.Printf("client %d thread %d worker=%d urls=%d", clientID, threadID, response.WorkerID, len(workerResponse.URLs))
				return
			}

			log.Printf(
				"client %d thread %d worker=%d urls=%s",
				clientID,
				threadID,
				response.WorkerID,
				string(urlsJSON),
			)
		}(threadID)
	}
	wg.Wait()

	if os.Getenv("KEEP_ALIVE") == "1" {
		select {}
	}
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
