package gomrjob

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"time"
)

var (
	stage      = flag.String("stage", "", "map,reduce")
	hdfsPrefix = flag.String("hdfs-prefix", "", "the hdfs://namenode/ prefix")
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
	Input   string
	Output  string
	tmpPath string
}

func NewRunner() *Runner {
	return &Runner{}
}

func (r *Runner) makeTempPath() {
	user, err := user.Current()
	var username = ""
	if err == nil {
		username = user.Username
	}
	now := time.Now().Format("20060102-150405")
	r.tmpPath = fmt.Sprintf("/user/%s/tmp/%s.%s", username, r.Name, now)
	Mkdir(r.tmpPath)
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

	r.makeTempPath()
	exe := fmt.Sprintf("%s/%s", r.tmpPath, "gomrjob_exe")
	exePath, err := filepath.EvalSymlinks("/proc/self/exe")
	if err != nil {
		log.Fatalf("failed stating file")
	}
	err = Put(exePath, exe)
	if err != nil {
		log.Fatalf("error running Put %s", err)
	}

	if r.Output == "" {
		r.Output = fmt.Sprintf("%s/output", r.tmpPath)
	}

	// submit a job
	err = SubmitJob(r.Name, r.Input, r.Output, exe)
	if err != nil {
		log.Fatalf("error SubmitJob %s", err)
	}

	return nil
}
