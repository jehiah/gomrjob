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
	"time"
)

var (
	stage        = flag.String("stage", "", "map,reduce")
	step         = flag.Int("step", 0, "the step to execute")
	remoteLogger = flag.String("remote-logger", "", "address for remote logger")
	submitJob    = flag.Bool("submit-job", false, "submit the job")
)

type Runner struct {
	Name               string
	Steps              []Step
	InputFiles         []string
	Output             string
	ReducerTasks       int
	PassThroughOptions []string
	tmpPath            string
	exePath            string
	MapReduceOptions   []string
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

func (r *Runner) submitJob(loggerAddress string, stepNumber int, step Step) error {
	if stepNumber >= len(r.Steps) || len(r.Steps) == 0 {
		return fmt.Errorf("step %d out of range", stepNumber)
	}
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

	// important for this to be the end of the genericOptions, and beginning of the command options
	// this allows users to specify "-D ....", or "-outputformat"
	for _, option := range r.MapReduceOptions {
		jobOptions = append(jobOptions, option)
	}

	name := r.Name
	if len(r.Steps) != 1 {
		name = fmt.Sprintf("%s-step_%d", name, stepNumber)
	}

	// specify reducer tasks per step
	reducerTasks := r.ReducerTasks
	if step, ok := step.(StepReducerTasksCount); ok {
		reducerTasks = step.NumberReducerTasks()
	}

	j := Job{
		Name:         name,
		CacheFiles:   []string{fmt.Sprintf("hdfs://%s#%s", r.exePath, processName)},
		ReducerTasks: reducerTasks,
		Input:        input,
		Output:       output,
		Mapper:       fmt.Sprintf("%s --stage=mapper", taskString),
		Reducer:      fmt.Sprintf("%s --stage=reducer", taskString),
		Options:      jobOptions,
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

// return which stage the runner is executing as
func (r *Runner) Stage() string {
	switch *stage {
	case "mapper", "reducer", "combiner":
		return *stage
	}
	if *submitJob {
		return "submit-job"
	}
	return "unknown"
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
			// if a step does not support mapper, it's the identity mapper of just echo std -> stdout
			_, err = io.Copy(os.Stdout, os.Stdin)
		} else {
			err = s.Mapper(os.Stdin, os.Stdout)
		}
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
		auditCpuTime("gomrjob", fmt.Sprintf("%s[%d]", *stage, *step))
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

	for stepNumber, step := range r.Steps {
		if err := r.submitJob(loggerAddress, stepNumber, step); err != nil {
			return fmt.Errorf("failed running Step %d = %s", stepNumber, err)
		}
	}

	return nil
}
