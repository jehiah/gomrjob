package gomrjob

import (
	"flag"
	"io"
	"log"
	"os"
)

var (
	stage = flag.String("stage", "", "map reduce step")
)

type Mapper interface {
	Run(io.Reader, io.Writer) error
}

type Reducer interface {
	Run(io.Reader, io.Writer) error
}

type Runner struct {
	Name    string
	Mapper  Mapper
	Reducer Reducer
}

func NewRunner() *Runner {
	return &Runner{}
}

func (r *Runner) Run() error {
	if *stage == "map" {
		return r.Mapper.Run(os.Stdin, os.Stdout)
	}
	if *stage == "reduce" {
		// todo pick based on step
		return r.Reducer.Run(os.Stdin, os.Stdout)
	}
	log.Printf("submitting a job")

	// submit a job
	// copy current file
	return nil

}
