package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string // 
	str   string
	bulk  string
	array []Value
}

type RESP struct {
	reader *bufio.Reader
}

func NewRESP(reader io.Reader) *RESP {
	return &RESP{reader: bufio.NewReader(reader)}
}

func (resp *RESP) readLine() (line []byte, n int, err error) {
	for {
		b, err := resp.reader.ReadByte()

		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (resp *RESP) readInteger() (x int, n int, err error) {
	line, n, err := resp.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (resp *RESP) Read() (Value, error) {
	_type, err := resp.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return resp.readArray()
	case BULK:
		return resp.readBulk()
	default:
		return Value{}, errors.New(fmt.Sprintln("Unknown type: ", string(_type)))
	}
}

func (resp *RESP) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	len, _, err := resp.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := resp.Read()
		if err != nil {
			return v, err
		}

		v.array = append(v.array, val)
	}

	return v, nil
}

func (resp *RESP) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	len, _, err := resp.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	resp.reader.Read(bulk)

	v.bulk = string(bulk)

	resp.readLine()

	return v, nil
}

func (v *Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) getKey() string {
	command := v.array[0].bulk

	if command == "HSET" || command == "HGET" {
		return fmt.Sprintf("%s/%s", v.array[1].bulk, v.array[2].bulk)
	}

	return v.array[1].bulk
}

func (v Value) splitCommandArgs() (string, []Value, error) {
	if v.typ != "array" {
		return "", nil, errors.New("expected a value of type 'array'")
	}

	command := strings.ToUpper(v.array[0].bulk)
	args := v.array[1:]

	return command, args, nil
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
