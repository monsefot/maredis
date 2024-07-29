package main

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     kset,
	"GET":     kget,
	"DELETE":  kdelete,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"MULTI":   empty,
	"COMMAND": empty,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func kset(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	defer SETsMu.Unlock()
	SETs[key] = value

	return Value{typ: "string", str: "OK"}
}

func kget(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk
	value, ok := SETs[key]

	if !ok {
		return Value{typ: "string", str: "null"}
	}

	return Value{typ: "string", str: value}

}

func kdelete(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'delete' command"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	defer SETsMu.Unlock()

	if _, ok := SETs[key]; ok {
		delete(SETs, key)
		return Value{typ: "string", str: "OK"}
	}

	return Value{typ: "error", str: "ERR key doesn't exist"}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.Mutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	value, ok := HSETs[hash][key]
	if !ok {
		return Value{typ: "error", str: "null"}
	}

	return Value{typ: "string", str: value}
}

func hgetall(args []Value) Value {
	values := make([]Value, 0)
	hash_exists := len(args) == 1

	for hash := range HSETs {
		if hash_exists && hash != args[0].bulk {
			continue
		}
		for _, value := range HSETs[hash] {
			values = append(values, Value{typ: "string", str: value})
		}
	}

	return Value{typ: "array", array: values}
}

func empty(args []Value) Value {
	return Value{typ: "string", str: ""}
}
