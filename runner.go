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
	"strings"
	"syscall"
	"time"
)

var (
	stage        = flag.String("stage", "", "map,reduce")
	step         = flag.Int("step", 0, "the step to execute")
	remoteLogger = flag.String("remote-logger", "", "address for remote logger")
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
	Reducer
}

type Runner struct {
	Name               string
	Steps              []Step
	InputFiles         []string
	Output             string
	ReducerTasks       int
	PassThroughOptions []string
	tmpPath            string
	exePath            string
	CompressOutput     bool
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
	return RMR(r.tmpPath)
}

func (r *Runner) submitJob(loggerAddress string, stepNumber int) error {
	if stepNumber >= len(r.Steps) || len(r.Steps) == 0 {
		return fmt.Errorf("step %d out of range", stepNumber)
	}
	step := r.Steps[stepNumber]
	var input []string
	var output string

	if stepNumber == len(r.Steps)-1 && r.Output != "" {
		output = r.Output
	} else {
		if len(r.Steps) == 1 {
			output = fmt.Sprintf("%s/output", r.tmpPath)
		} else {
			output = fmt.Sprintf("%s/step_%d/output", r.tmpPath, stepNumber)
		}
	}

	if stepNumber == 0 {
		input = r.InputFiles
	} else {
		input = append(input, fmt.Sprintf("%s/step_%d/output/part-*", r.tmpPath, stepNumber-1))
	}

	processName := filepath.Base(r.exePath)
	taskOptions := r.PassThroughOptions[:]
	if loggerAddress != "" {
		taskOptions = append(taskOptions, fmt.Sprintf("--remote-logger=%s", loggerAddress))
	}
	taskOptions = append(taskOptions, fmt.Sprintf("--step=%d", stepNumber))
	taskString := fmt.Sprintf("%s %s", processName, strings.Join(taskOptions, " "))

	var jobOptions []string
	if r.CompressOutput {
		jobOptions = append(jobOptions, "-D", "mapred.output.compress=true")
		jobOptions = append(jobOptions, "-D", "mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
	}

	j := Job{
		Name:         r.Name,
		CacheFiles:   []string{fmt.Sprintf("hdfs://%s#%s", r.exePath, processName)},
		ReducerTasks: r.ReducerTasks,
		Input:        input,
		Output:       output,
		Reducer:      fmt.Sprintf("%s --stage=reducer", taskString),
		Options:      jobOptions,
	}
	if _, ok := step.(Mapper); ok {
		j.Mapper = fmt.Sprintf("%s --stage=mapper", taskString)
	}
	if _, ok := step.(Combiner); ok {
		j.Combiner = fmt.Sprintf("%s --stage=combiner", taskString)
	}
	return SubmitJob(j)
}

func (r *Runner) copyRunningBinaryToHdfs() error {
	// copy the current executible binary to hadoop for use as the map reduce tasks
	r.exePath = fmt.Sprintf("%s/%s", r.tmpPath, "gomrjob_binary")
	localExePath, err := filepath.EvalSymlinks("/proc/self/exe")
	if err != nil {
		return fmt.Errorf("failed locating running executable %s", err)
	}
	if err := Put(localExePath, r.exePath); err != nil {
		return fmt.Errorf("error copying %s to hdfs %s", r.exePath, err)
	}
	return nil
}

func (r *Runner) auditCpuTime(stage string) {
	var u syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	if err != nil {
		log.Printf("error getting Rusage: %s", err)
		return
	}
	userTime := time.Duration(u.Utime.Nano()) * time.Nanosecond
	systemTime := time.Duration(u.Stime.Nano()) * time.Nanosecond
	Counter("gomrjob", fmt.Sprintf("%s userTime (ms)", stage), int64(userTime/time.Millisecond))
	Counter("gomrjob", fmt.Sprintf("%s systemTime (ms)", stage), int64(systemTime/time.Millisecond))
}

func (r *Runner) Run() error {
	if *step >= len(r.Steps) {
		return fmt.Errorf("invalid --step=%d (max %d)", *step, len(r.Steps))
	}
	if *remoteLogger != "" {
		conn, err := dialRemoteLogger(*remoteLogger)
		if err != nil {
			if *stage == "" {
				Status(fmt.Sprintf("error dialing remote logger %s", err))
			} else {
				log.Printf("failed connecting to remote logger", err)
			}
		} else {
			hostname, _ := os.Hostname()
			w := newPrefixLogger(fmt.Sprintf("[%s %s:%d] ", hostname, *stage, *step), conn)
			log.SetOutput(w)
		}
	}
	s := r.Steps[*step]

	if *stage != "" {
		log.Printf("starting %s step %d", *stage, *step)
	}
	var err error
	switch *stage {
	case "mapper":
		s, ok := s.(Mapper)
		if !ok {
			return errors.New("step does not support Mapper interface")
		}
		err = s.Mapper(os.Stdin, os.Stdout)
	case "reducer":
		err = s.Reducer(os.Stdin, os.Stdout)
	case "combiner":
		s, ok := s.(Combiner)
		if !ok {
			return errors.New("step does not support Combiner interface")
		}
		err = s.Combiner(os.Stdin, os.Stdout)
	}
	if *stage != "" {
		r.auditCpuTime(fmt.Sprintf("%s[%d]", *stage, *step))
		if err != nil {
			log.Printf("Error: %s", err)
			os.Exit(1)
		} else {
			os.Exit(0)
		}
		return nil
	}
	if !*submitJob {
		return errors.New("missing --submit-job")
	}

	log.Printf("submitting map reduce job")

	r.setTempPath()
	if err := Mkdir(r.tmpPath); err != nil {
		return err
	}

	if err := r.copyRunningBinaryToHdfs(); err != nil {
		return err
	}

	loggerAddress := startRemoteLogListner()

	if r.Output == "" {
		r.Output = fmt.Sprintf("%s/output", r.tmpPath)
	}

	for stepNumber, _ := range r.Steps {
		if err := r.submitJob(loggerAddress, stepNumber); err != nil {
			return fmt.Errorf("failed running Step %d = %s", stepNumber, err)
		}
	}

	return nil
}
