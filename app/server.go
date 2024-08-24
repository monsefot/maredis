package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Server struct {
	listener net.Listener
	aof      *AOF
	pipe     *Pipe
	raft     *raft.Raft
}

func (server *Server) Snapshot() (raft.FSMSnapshot, error) {
	return &ServerSnapshot{}, nil
}

// ServerSnapshot represents a snapshot of the server's state
type ServerSnapshot struct{}

// Persist is used to persist the snapshot to disk
func (f *ServerSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// Release is called when the snapshot is no longer needed
func (f *ServerSnapshot) Release() {}

func (server *Server) Restore(snapshot io.ReadCloser) error {
	return nil
}

func NewServer() (*Server, error) {

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%s", os.Getenv("REDIS_PORT")))
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

func (server *Server) SetupRaftConfiguration() error {

	fmt.Println(os.Getenv("NODE_ID"))
	// create raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(os.Getenv("NODE_ID"))

	// create new BoltDB store for raft
	logStore, err := raftboltdb.NewBoltStore("raft-log.bolt")
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore("raft-stable.bolt")
	if err != nil {
		return err
	}

	snapshotStore := raft.NewDiscardSnapshotStore()

	// setup network transport
	transport, err := raft.NewTCPTransport(fmt.Sprintf("127.0.0.1:%s", os.Getenv("RAFT_PORT")), nil, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return err
	}

	// create Raft

	raftNode, err := raft.NewRaft(config, server, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	// Bootstrap the cluster (if necessary)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      "master",
				Address: "127.0.0.1:8081",
			},
			{
				ID:      "slave",
				Address: "127.0.0.1:8083",
			},
		},
	}

	raftNode.BootstrapCluster(configuration)
	server.raft = raftNode
	return nil
}

func (server *Server) Apply(log *raft.Log) interface{} {
	fmt.Println("[RAFT] Applying log:", log.Index)

	// Decode the log entry and apply it to the server's state
	var value Value
	err := json.Unmarshal(log.Data, &value)
	if err != nil {
		return err
	}

	// Apply the command to your in-memory state
	command, args, err := value.splitCommandArgs()
	if err != nil {
		return err
	}

	if command == "SET" || command == "HSET" {
		fmt.Println("[RAFT] Writing to AOF on node", os.Getenv("NODE_ID"))
		server.aof.Write(value)
	}

	handler, ok := Handlers[command]
	if !ok {
		return fmt.Errorf("unknown command: %s", command)
	}

	response := handler(args)
	return response
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

	if command != "GET" && command != "HGET" {
		// apply the changes to the Raft log
		future := server.raft.Apply(value.Marshal(), 10*time.Second)
		if future.Error() != nil {
			return Value{}, future.Error()
		}
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
