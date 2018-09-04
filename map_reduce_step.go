package gomrjob

import (
	"io"
)

type Mapper interface {
	Mapper(io.Reader, io.Writer) error
}

type Reducer interface {
	Reducer(io.Reader, io.Writer) error
}

type Combiner interface {
	Combiner(io.Reader, io.Writer) error
}

type StepReducerTasksCount interface {
	NumberReducerTasks() int
}

type Step interface {
	Reducer
}
