package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file           *os.File
	reader         *bufio.Reader
	mu             sync.Mutex
	indexKeys      map[string][2]int64
	current_offset int64
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file:           f,
		reader:         bufio.NewReader(f),
		indexKeys:      map[string][2]int64{},
		current_offset: int64(0),
	}

	// Start a goroutine to sync AOF to disk every 1 second
	go func() {
		for {
			aof.mu.Lock()

			aof.file.Sync()

			aof.mu.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *AOF) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	key := value.getKey()
	if offsets, ok := aof.indexKeys[key]; ok {
		buffer := value.Marshal()
		_, err := aof.file.WriteAt(buffer[:len(buffer)-2], offsets[0])

		if err != nil {
			return err
		}

	} else {
		buffer := value.Marshal()
		_, err := aof.file.Write(buffer)

		if err != nil {
			return err
		}

		aof.indexKeys[key] = [2]int64{aof.current_offset, aof.current_offset + int64(len(buffer))}
		aof.current_offset += int64(len(buffer))
	}

	return nil
}

func (aof *AOF) Read(callback func(value Value)) {
	aof.file.Sync()
	resp := NewRESP(aof.reader)
	aof.current_offset = 0

	for {
		value, err := resp.Read()
		if err != nil {
			if err != io.EOF {
				fmt.Println("[AOF_ERR] ", err)
			}
			return
		}

		callback(value)

		buffer := value.Marshal()
		aof.indexKeys[value.getKey()] = [2]int64{aof.current_offset, aof.current_offset + int64(len(buffer))}
		aof.current_offset += int64(len(buffer))
		fmt.Println("[AOF] current offset: ", aof.current_offset)
	}
}

func (aof *AOF) Delete(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	key := value.getKey()
	offsets, exists := aof.indexKeys[key]

	if !exists {
		return errors.New("[AOFERR] missed offset")
	}
	buffer_size := aof.current_offset - offsets[1]
	buffer := make([]byte, buffer_size)

	aof.file.ReadAt(buffer, offsets[1])

	_, err := aof.file.Seek(offsets[0], io.SeekStart)
	if err != nil {
		return err
	}

	err = aof.file.Truncate(offsets[1])
	if err != nil {
		return err
	}

	_, err = aof.file.Write(buffer)
	if err != nil {
		return err
	}

	_, err = aof.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	aof.Read(func(value Value) {})

	return nil
}
