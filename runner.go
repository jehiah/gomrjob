package gomrjob

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/jehiah/gomrjob/dataproc"
	"github.com/jehiah/gomrjob/hdfs"
	"github.com/jehiah/gomrjob/internal/gcloud"
	"github.com/jehiah/gomrjob/internal/storage"
)

var (
	submitJob = flag.Bool("submit-job", false, "submit the job")

	// internal flags used by the map-reduce steps
	stage        = flag.String("stage", "", "map,reduce")
	step         = flag.Int("step", 0, "the step to execute")
	remoteLogger = flag.String("remote-logger", "", "address for remote logger")

	// flags for Dataproc support
	bucket         = flag.String("bucket", "", "Google Storage bucket to use | GS_BUCKET")
	project        = flag.String("project", "", "Google Cloud Project ID | GS_PROJECT")
	cluster        = flag.String("cluster", "", "Dataproc cluster | GS_CLUSTER")
	region         = flag.String("region", "", "Dataproc region | GS_REGION")
	serviceAccount = flag.String("service_account", "", "Google Storage Service Account JSON | GOOGLE_APPLICATION_CREDENTIALS")
)

type JobType int8

const (
	HDFS JobType = iota
	Dataproc
)

const executibleName = "gomrjob_binary" // The filenamename used for the executible when uploaded

type Runner struct {
	Name  string
	Steps []Step
	// Inputfiles can be of the format `/pattern/to/files*.gz` or `hdfs:///pattern/to/files*.gz` or `s3://bucket/pattern`
	InputFiles         []string
	Output             string // fully qualified
	ReducerTasks       int
	PassThroughOptions []string // CLI arguments to $exe when run as map / reduce tasks
	CompressOutput     bool
	CacheFiles         []string          // -files
	Files              []string          // -file
	Properties         map[string]string // -D key=value argumets to mapreduce-streaming.jar
	JobType            JobType

	defaultProto string
	tmpPath      string
	gcloud       *http.Client
}

// LoadAndValidateFlags loads flags from env and checks for missing arguments
func LoadAndValidateFlags() {
	// bootstrap missing flags from environment (if set)
	for env, target := range map[string]*string{
		"GOOGLE_APPLICATION_CREDENTIALS": serviceAccount,
		"GS_REGION":                      region,
		"GS_PROJECT":                     project,
		"GS_CLUSTER":                     cluster,
		"GS_BUCKET":                      bucket,
	} {
		if have := os.Getenv(env); have != "" && *target == "" {
			*target = have
		}
	}

	if *serviceAccount == "" {
		return
	}

	switch {
	case *project == "":
		log.Fatal("missing --project")
	case *cluster == "":
		log.Fatal("missing --cluster")
	case *region == "":
		log.Fatal("missing --region")
	case *bucket == "":
		log.Fatal("missing --bucket")
	}
}

func NewRunner() *Runner {
	r := &Runner{
		ReducerTasks: 30,
		Properties:   make(map[string]string),
		JobType:      HDFS,
		defaultProto: "hdfs:///",
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
	r.tmpPath = fmt.Sprintf("user/%s/tmp/%s.%s", username, r.Name, now)
}

func (r *Runner) Cleanup() error {
	switch r.JobType {
	case HDFS:
		return hdfs.RMR(r.tmpPath)
	case Dataproc:
		return storage.DeletePrefix(context.Background(), r.gcloud, *bucket, r.tmpPath)
	}
	panic("invalid job type")
}

// submitJob runs a single map/combine/reduce job.
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

	taskOptions := append([]string{executibleName}, r.PassThroughOptions...)
	if loggerAddress != "" {
		taskOptions = append(taskOptions, fmt.Sprintf("--remote-logger=%s", loggerAddress))
	}
	taskOptions = append(taskOptions, fmt.Sprintf("--step=%d", stepNumber))
	taskString := strings.Join(taskOptions, " ")

	if r.CompressOutput {
		r.Properties["mapred.output.compress"] = "true"
		r.Properties["mapred.output.compression.codec"] = "org.apache.hadoop.io.compress.GzipCodec"
	}

	name := r.Name
	if len(r.Steps) != 1 {
		name = fmt.Sprintf("%s-step_%d", name, stepNumber)
	}

	// StepReducerTasksCount interface overrides reducer tasks per step
	reducerTasks := r.ReducerTasks
	if step, ok := step.(StepReducerTasksCount); ok {
		reducerTasks = step.NumberReducerTasks()
	}

	j := hdfs.Job{
		Name:         name,
		ReducerTasks: reducerTasks,
		Input:        input,
		Output:       output,
		Mapper:       fmt.Sprintf("%s --stage=mapper", taskString),
		Reducer:      fmt.Sprintf("%s --stage=reducer", taskString),
		Files:        r.Files,
		Properties:   r.Properties,
		CacheFiles:   r.CacheFiles,
		DefaultProto: r.defaultProto,
	}
	if _, ok := step.(Combiner); ok {
		j.Combiner = fmt.Sprintf("%s --stage=combiner", taskString)
	}
	switch r.JobType {
	case HDFS:
		return hdfs.SubmitJob(j)
	case Dataproc:
		return dataproc.SubmitJob(j, r.gcloud, *project, *region, *cluster)
	default:
		panic("unknown job type")
	}
}

func (r *Runner) copyRunningBinaryToHdfs() error {
	// copy the current executible binary to hadoop for use as the map reduce tasks
	localExePath, err := filepath.EvalSymlinks("/proc/self/exe")
	if err != nil {
		return fmt.Errorf("failed locating running executable %s", err)
	}
	exePath := fmt.Sprintf("%s/%s", r.tmpPath, executibleName)
	if err := hdfs.Put(localExePath, exePath); err != nil {
		return fmt.Errorf("error copying %s to hdfs %s", exePath, err)
	}
	r.CacheFiles = append(r.CacheFiles, fmt.Sprintf("%s%s#%s", r.defaultProto, exePath, executibleName))
	return nil
}

func (r *Runner) cacheFileInGoogleStorage(ctx context.Context, src, target string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	cachedFile := fmt.Sprintf("gs://%s/%s", *bucket, target)
	log.Printf("uploading %s as %s", src, cachedFile)

	var contentType string
	err = storage.Insert(ctx, r.gcloud, *bucket, target, contentType, f)
	if err != nil {
		return err
	}

	r.CacheFiles = append(r.CacheFiles, cachedFile)
	return nil
}

func (r *Runner) copyRunningBinaryToDataproc(ctx context.Context) error {
	exePath := fmt.Sprintf("%s/%s", r.tmpPath, executibleName)
	return r.cacheFileInGoogleStorage(ctx, "/proc/self/exe", exePath)
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

// Run is the program entry point from main()
//
// When executed directly (--stage=”) uploads loads the executibile
// and submits mapreduce jobs for each stage of the program
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
				log.Printf("failed connecting to remote logger %s", err)
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

	r.setTempPath()
	LoadAndValidateFlags()
	if *serviceAccount != "" {
		r.gcloud, err = gcloud.LoadFromServiceJSON(*serviceAccount, gcloud.ScopeCloudPlatform, gcloud.ScopeStorageReadWrite)
		if err != nil {
			log.Fatal(err)
		}
		r.JobType = Dataproc
		r.defaultProto = fmt.Sprintf("gs://%s/", *bucket)
	}

	switch r.JobType {
	case HDFS:
		if err := hdfs.FsCmd("-mkdir", "-p", r.defaultProto+r.tmpPath); err != nil {
			return err
		}
		if err := r.copyRunningBinaryToHdfs(); err != nil {
			return err
		}
	case Dataproc:
		ctx := context.Background()
		if err := r.copyRunningBinaryToDataproc(ctx); err != nil {
			return err
		}
		// since -file on the hadoop-streaming.jar submission doesn't refer to a local file
		// we need to upload the files to Google Storage and use CacheFiles
		for _, f := range r.Files {
			target := fmt.Sprintf("%s/%s", r.tmpPath, filepath.Base(f))
			if err := r.cacheFileInGoogleStorage(ctx, f, target); err != nil {
				return err
			}
		}
		r.Files = []string{}
	}

	loggerAddress := startRemoteLogListner()

	if r.Output == "" {
		r.Output = fmt.Sprintf("%s%s/output", r.defaultProto, r.tmpPath)
	}

	for stepNumber, step := range r.Steps {
		if err := r.submitJob(loggerAddress, stepNumber, step); err != nil {
			return fmt.Errorf("failed running Step %d = %s", stepNumber, err)
		}
	}

	return nil
}
