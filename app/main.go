package main

import (
	"fmt"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("[ERROR] .ENV ", err)
		return
	}

	server, err := NewServer()
	if err != nil {
		fmt.Println("[ERROR] ", err)
	}

	err = server.SetupRaftConfiguration()
	if err != nil {
		fmt.Println("[ERROR] Failed to setup Raft: ", err)
	}

	server.LoadState()
	server.AcceptIncommingRequests()
}
