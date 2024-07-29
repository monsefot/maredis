package main

import (
	"errors"
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

	// open or create (if it doesn't exist) the commands log file
	aof, err := NewAOF("database.aof")
	if err != nil {
		fmt.Println("[ERR] ", err)
		return
	}

	// read the cammands one by one and execute them.
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

	pipe := NewPipe()

	// receive, process and execute commands.
	for {
		resp := NewRESP(connection)
		value, err := resp.Read()

		if err != nil {
			fmt.Println("[RESP_ERR] ", err)
			return
		}

		if value.typ != "array" {
			fmt.Println("[RESP_ERR] Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("[RESP_ERR] Invalid request, expected array length > 0")
			continue
		}

		writer := NewWriter(connection)
		command, args, err := value.splitCommandArgs()

		if err != nil {
			fmt.Println("[RESP_ERR] ", err)
			continue
		}
		switch command {
		case "MULTI":
			{

				pipe.activated = true
				writer.Write(Value{typ: "string", str: ""})
			}
		case "EXEC":
			{
				if !pipe.activated {
					break
				}
				results := []Value{}
				for _, value := range pipe.queue {
					command, args, err := value.splitCommandArgs()
					if err != nil {
						fmt.Println("[RESP_ERR] ", err)
					}

					result, err := handleRequest(writer, aof, value, command, args)
					if err != nil {
						fmt.Println("ERR", err)
						results = append(results, Value{typ: "error", str: err.Error()})
						continue
					}
					results = append(results, result)
				}
				pipe.queue = pipe.queue[:0]
				pipe.activated = false
				writer.Write(Value{typ: "array", array: results})

			}
		default:
			{
				if pipe.activated {
					pipe.queue = append(pipe.queue, value)
					writer.Write(Value{typ: "string", str: "QUEUED"})
					break
				}

				response, err := handleRequest(writer, aof, value, command, args)
				if err != nil {
					fmt.Println("[ERR] ", err)
					continue
				}

				writer.Write(response)
			}
		}
	}
}

func handleRequest(writer *Writer, aof *AOF, value Value, command string, args []Value) (Value, error) {

	handler, ok := Handlers[command]

	if !ok {
		writer.Write(Value{typ: "string", str: ""})
		return Value{}, errors.New(fmt.Sprint("[RESP_ERR] Invalid command : ", command))
	}

	if command == "SET" || command == "HSET" {
		aof.Write(value)
	}

	if command == "DELETE" {
		aof.Delete(value)
	}

	return handler(args), nil
}
