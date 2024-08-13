package main

type Pipe struct {
	queue     []Value
	activated bool
}

func NewPipe() *Pipe {
	return &Pipe{queue: make([]Value, 0), activated: false}
}
