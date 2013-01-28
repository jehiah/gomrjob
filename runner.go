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
	step         = flag.Int("step", 0, "the step to execute")
	remoteLogger = flag.String("remote-logger", "", "address for remote logger")
	hdfsPrefix   = flag.String("hdfs-prefix", "", "the hdfs://namenode/ prefix")
	submitJob    = flag.Bool("submit-job", false, "submit the job")
)

type Mapper interface {
	MapperSetup() error
	Mapper(io.Reader, io.Writer) error
	MapperTeardown(io.Writer) error
}

type Reducer interface {
	ReducerSetup() error
	Reducer(io.Reader, io.Writer) error
	ReducerTeardown(io.Writer) error
}

type Step interface {
	Mapper
	Reducer
}

type Runner struct {
	Name       string
	Steps      []Step
	InputFiles []string
	Output     string
	tmpPath    string
}

func NewRunner() *Runner {
	r := &Runner{}
	r.setTempPath()
	return r
}

func (r *Runner) setTempPath() {
	user, err := user.Current()
	var username = ""
	if err == nil {
		username = user.Username
	}
	now := time.Now().Format("20060102-150405")
	r.tmpPath = fmt.Sprintf("/user/%s/tmp/%s.%s", username, r.Name, now)
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
	if *step >= len(r.Steps) {
		return fmt.Errorf("invalid --step=%d (max %d)", *step, len(r.Steps))
	}
	s := r.Steps[*step]

	switch *stage {
	case "map", "mapper":
		if err := s.MapperSetup(); err != nil {
			return err
		}
		if err := s.Mapper(os.Stdin, os.Stdout); err != nil {
			return err
		}
		if err := s.MapperTeardown(os.Stdout); err != nil {
			return err
		}
	case "reduce":
		if err := s.ReducerSetup(); err != nil {
			return err
		}
		if err := s.Reducer(os.Stdin, os.Stdout); err != nil {
			return err
		}
		if err := s.ReducerTeardown(os.Stdout); err != nil {
			return err
		}
	}
	if !*submitJob {
		return errors.New("missing --submit-job or --stage")
	}

	log.Printf("submitting a job")

	r.setTempPath()
	Mkdir(r.tmpPath)
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
	err = SubmitJob(r.Name, r.InputFiles, r.Output, loggerAddress, exe)
	if err != nil {
		log.Fatalf("error SubmitJob %s", err)
	}

	return nil
}
