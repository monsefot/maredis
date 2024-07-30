package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

const PORT = 6379

type Server struct {
	listener   net.Listener
	connection net.Conn
	aof        *AOF
	resp       *RESP
	pipe       *Pipe
	writer     *Writer
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
		listener:   listener,
		connection: nil,
		aof:        aof,
		resp:       nil,
		pipe:       NewPipe(),
		writer:     nil,
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
	connection, err := server.listener.Accept()
	if err != nil {
		return err
	}
	server.connection = connection
	return nil
}

func (server *Server) ProcessRequest() {
	for {
		server.resp = NewRESP(server.connection)
		value, err := server.resp.Read()

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
		server.writer = NewWriter(server.connection)
		command, args, err := value.splitCommandArgs()

		if err != nil {
			fmt.Println("[ERROR][RESP] ", err)
			continue
		}

		switch command {
		case "MULTI":
			{

				server.pipe.activated = true
				server.writer.Write(Value{typ: "string", str: ""})
			}
		case "EXEC":
			{
				if !server.pipe.activated {
					break
				}
				server.ExecutePipe()
			}
		default:
			{
				if server.pipe.activated {
					server.pipe.queue = append(server.pipe.queue, value)
					server.writer.Write(Value{typ: "string", str: "QUEUED"})
					break
				}

				response, err := server.HandleRequest(value, command, args)
				if err != nil {
					fmt.Println("[ERR] ", err)
					continue
				}

				server.writer.Write(response)
			}
		}

	}
}

func (server *Server) HandleRequest(value Value, command string, args []Value) (Value, error) {
	handler, ok := Handlers[command]

	if !ok {
		server.writer.Write(Value{typ: "string", str: ""})
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

func (server *Server) ExecutePipe() {
	results := []Value{}
	for _, value := range server.pipe.queue {
		command, args, err := value.splitCommandArgs()
		if err != nil {
			fmt.Println("[RESP_ERR] ", err)
		}

		result, err := server.HandleRequest(value, command, args)
		if err != nil {
			fmt.Println("ERR", err)
			results = append(results, Value{typ: "error", str: err.Error()})
			continue
		}
		results = append(results, result)
	}
	server.pipe.queue = server.pipe.queue[:0]
	server.pipe.activated = false
	server.writer.Write(Value{typ: "array", array: results})

}
