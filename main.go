package main

import (
	"fmt"
	"os"
	"strconv"

	"distributed_search_engine/client"
	"distributed_search_engine/coordinator"
	"distributed_search_engine/worker"
)

const MaxVisitedUrls int = 900
const R int = 4
const W int = 4
const OutputReplicas int = 1

func main() {
	role := getEnvString("ROLE", "coordinator")
	r := getEnvInt("NUM_REDUCE_TASKS", R)
	w := getEnvInt("NUM_WORKERS", W)
	maxVisitedUrls := getEnvInt("MAX_VISITED_URLS", MaxVisitedUrls)
	outputReplicas := getEnvInt("NUM_OUTPUT_REPLICAS", OutputReplicas)

	switch role {
	case "coordinator":
		var file *os.File
		var err error
		if os.Getenv("PRE_SPLIT_INPUT") != "1" {
			file, err = os.Open("seed_urls.txt")
			if err != nil {
				panic("failed to read input file for coordinator")
			}
			defer file.Close()
		}

		coordinator.StartCoordinator(r, w, maxVisitedUrls, outputReplicas, file)
	case "worker":
		workerID := getEnvInt("WORKER_ID", 1)
		workerAddr := getEnvString("WORKER_ADDR", fmt.Sprintf("127.0.0.1:%d", 9100+workerID))
		workerBindAddr := getEnvString("WORKER_BIND_ADDR", fmt.Sprintf("0.0.0.0:%d", 9100+workerID))
		coordinatorAddr := getEnvString("COORDINATOR_ADDR", "127.0.0.1:9000")
		dataDir := getEnvString("WORKER_DATA_DIR", fmt.Sprintf("data/worker-%d", workerID))

		worker.StartWorker(workerID, workerBindAddr, workerAddr, coordinatorAddr, dataDir)
	case "client":
		clientID := getEnvInt("CLIENT_ID", 1)
		coordinatorAddr := getEnvString("COORDINATOR_ADDR", "127.0.0.1:9000")

		client.StartClient(clientID, coordinatorAddr)
	default:
		panic("unsupported ROLE: " + role)
	}
}

func getEnvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		panic("invalid integer value for " + name)
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
