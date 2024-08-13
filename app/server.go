package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

const PORT = 6379

type Server struct {
	listener net.Listener
	aof      *AOF
	pipe     *Pipe
}

func NewServer() (*Server, error) {

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		return nil, err
	}

	aof, err := NewAOF("database.aof")
	if err != nil {
		return nil, err
	}

	return &Server{
		listener: listener,
		aof:      aof,
		pipe:     NewPipe(),
	}, nil

}

func (server *Server) LoadState() {
	server.aof.Read(func(value Value) {
		if value.typ != "array" {
			fmt.Println("[ERROR][AOF] Expected a value of type 'array'.")
			return
		}
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("[ERROR][AOF] Invalid command: ", command)
			return
		}

		handler(args)
		fmt.Println("[AOF] Executed ", command)
	})
}

func (server *Server) AcceptIncommingRequests() error {
	fmt.Println("[OK] Server started listening on ", server.listener.Addr().String())

	for {

		connection, err := server.listener.Accept()
		if err != nil {
			return err
		}

		go func(connection net.Conn) {
			defer connection.Close()
			server.ProcessRequest(connection)
		}(connection)
	}
}

func (server *Server) ProcessRequest(connection net.Conn) {
	for {
		resp := NewRESP(connection)
		value, err := resp.Read()

		if err != nil {
			fmt.Println("[ERROR][RESP] ", err)
			return
		}

		if value.typ != "array" {
			fmt.Println("[ERROR][RESP] Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("[ERROR][RESP] Invalid request, expected array length > 0")
			continue
		}
		writer := NewWriter(connection)
		command, args, err := value.splitCommandArgs()

		if err != nil {
			fmt.Println("[ERROR][RESP] ", err)
			continue
		}

		switch command {
		case "MULTI":
			{

				server.pipe.activated = true
				writer.Write(Value{typ: "string", str: ""})
			}
		case "EXEC":
			{
				if !server.pipe.activated {
					break
				}
				server.ExecutePipe(writer)
			}
		default:
			{
				if server.pipe.activated {
					server.pipe.queue = append(server.pipe.queue, value)
					writer.Write(Value{typ: "string", str: "QUEUED"})
					break
				}

				response, err := server.HandleRequest(writer, value, command, args)
				if err != nil {
					fmt.Println("[ERR] ", err)
					continue
				}

				writer.Write(response)
			}
		}

	}
}

func (server *Server) HandleRequest(writer *Writer, value Value, command string, args []Value) (Value, error) {
	handler, ok := Handlers[command]

	if !ok {
		writer.Write(Value{typ: "string", str: ""})
		return Value{}, errors.New(fmt.Sprint("[RESP_ERR] Invalid command : ", command))
	}

	if command == "SET" || command == "HSET" {
		server.aof.Write(value)
	}

	if command == "DELETE" {
		server.aof.Delete(value)
	}

	return handler(args), nil
}

func (server *Server) ExecutePipe(writer *Writer) {
	results := []Value{}
	for _, value := range server.pipe.queue {
		command, args, err := value.splitCommandArgs()
		if err != nil {
			fmt.Println("[RESP_ERR] ", err)
		}

		result, err := server.HandleRequest(writer, value, command, args)
		if err != nil {
			fmt.Println("ERR", err)
			results = append(results, Value{typ: "error", str: err.Error()})
			continue
		}
		results = append(results, result)
	}
	server.pipe.queue = server.pipe.queue[:0]
	server.pipe.activated = false
	writer.Write(Value{typ: "array", array: results})

}
