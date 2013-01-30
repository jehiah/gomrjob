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
	Mapper(io.Reader, io.Writer) error
}

type Reducer interface {
	Reducer(io.Reader, io.Writer) error
}

type Combiner interface {
	Combiner(io.Reader, io.Writer) error
}

type Step interface {
	Mapper
	Reducer
}

type Runner struct {
	Name         string
	Steps        []Step
	InputFiles   []string
	Output       string
	ReducerTasks int
	tmpPath      string
}

func NewRunner() *Runner {
	r := &Runner{
		ReducerTasks: 30,
	}
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

func (r *Runner) Cleanup() error {
	return RMR(r.Output)
}

func (r *Runner) submitJob(loggerAddress string, processName string, combiner bool) error {
	remoteLogger := ""
	if loggerAddress != "" {
		remoteLogger = fmt.Sprintf(" --remote-logger=%s", loggerAddress)
	}
	j := Job{
		Name:         r.Name,
		CacheFiles:   []string{fmt.Sprintf("hdfs://%s#%s", processName, filepath.Base(processName))},
		ReducerTasks: r.ReducerTasks,
		Input:        r.InputFiles,
		Output:       r.Output,
		Mapper:       fmt.Sprintf("%s --stage=mapper%s", filepath.Base(processName), remoteLogger),
		Reducer:      fmt.Sprintf("%s --stage=reducer%s", filepath.Base(processName), remoteLogger),
	}
	if combiner {
		j.Combiner = fmt.Sprintf("%s --stage=combiner%s", filepath.Base(processName), remoteLogger)
	}
	return SubmitJob(j)
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
	case "mapper":
		if err := s.Mapper(os.Stdin, os.Stdout); err != nil {
			return err
		}
		// we want execution to finish here, so just exit.
		os.Exit(0)
		return nil
	case "reducer":
		if err := s.Reducer(os.Stdin, os.Stdout); err != nil {
			return err
		}
		// we want execution to finish here, so just exit.
		os.Exit(0)
		return nil
	case "combiner":
		s := s.(Combiner)
		if err := s.Combiner(os.Stdin, os.Stdout); err != nil {
			return err
		}
		// we want execution to finish here, so just exit.
		os.Exit(0)
		return nil
	}
	if !*submitJob {
		return errors.New("missing --submit-job or --stage")
	}

	log.Printf("submitting map reduce job")

	r.setTempPath()
	Mkdir(r.tmpPath)

	// copy the current executible binary to hadoop for use as the map reduce tasks
	exe := fmt.Sprintf("%s/%s", r.tmpPath, "gomrjob_binary")
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
	_, combiner := s.(Combiner)
	// submit the streaming job
	err = r.submitJob(loggerAddress, exe, combiner)
	if err != nil {
		log.Fatalf("error submitting job %s", err)
	}

	return nil
}
