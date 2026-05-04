package client

import (
	"log"
	"os"
)

func StartClient(clientID int, coordinatorAddr string) {
	log.Printf("client %d ready coordinator=%s", clientID, coordinatorAddr)

	if os.Getenv("KEEP_ALIVE") == "1" {
		select {}
	}
}
