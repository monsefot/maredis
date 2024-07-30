package main

import (
	"fmt"
)

func main() {
	server, err := NewServer()
	if err != nil {
		fmt.Println("[ERROR] ", err)
	}

	server.LoadState()
	server.AcceptIncommingRequests()
	defer server.connection.Close()

	server.ProcessRequest()
}
