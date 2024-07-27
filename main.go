package main

import (
	"fmt"
	"net"
	"strings"
)

const PORT = 6379
const BUFFER_SIZE = 1024

func main() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		fmt.Println("[ERR] ", err)
		return
	}

	aof, err := NewAOF("database.aof")
	if err != nil {
		fmt.Println("[ERR] ", err)
		return
	}

	aof.Read(func(value Value) {
		if value.typ != "array" {
			return
		}
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("[AOFERR] Invalid command: ", command)
			return
		}

		handler(args)
		fmt.Println("[AOF] Executed ", command)
	})

	fmt.Println("[OK] Server started listening on ", listener.Addr().String())

	connection, err := listener.Accept()
	if err != nil {
		fmt.Println("[ERR] ", err)
		return
	}
	fmt.Println("[OK] Client connected !")

	defer connection.Close()

	for {
		resp := NewRESP(connection)
		value, err := resp.Read()

		if err != nil {
			fmt.Println("[ERR] ", err)
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		value.array[0].bulk = strings.ToUpper(value.array[0].bulk)
		command := value.array[0].bulk
		args := value.array[1:]

		writer := NewWriter(connection)

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command : ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		if command == "DELETE" {
			aof.Delete(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
