package gomrjob

import (
	"errors"
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
	stage        = flag.String("stage", "", "map,reduce")
	remoteLogger = flag.String("remote-logger", "", "address for remote logger")
	hdfsPrefix   = flag.String("hdfs-prefix", "", "the hdfs://namenode/ prefix")
	submitJob    = flag.Bool("submit-job", false, "submit the job")
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
	if *remoteLogger != "" {
		err := dialRemoteLogger(*remoteLogger)
		if err != nil {
			if *stage == "" {
				Status(fmt.Sprintf("error dialing remote logger %s", err))
			} else {
				log.Printf("failed connecting to remote logger", err)
			}
		}
	}
	if *stage == "map" {
		return r.Mapper.Run(os.Stdin, os.Stdout)
	}
	if *stage == "reduce" {
		// todo pick based on step
		return r.Reducer.Run(os.Stdin, os.Stdout)
	}

	if !*submitJob {
		return errors.New("missing --submit-job or --stage")
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

	loggerAddress := startRemoteLogListner()
	// submit a job
	err = SubmitJob(r.Name, r.Input, r.Output, loggerAddress, exe)
	if err != nil {
		log.Fatalf("error SubmitJob %s", err)
	}

	return nil
}
