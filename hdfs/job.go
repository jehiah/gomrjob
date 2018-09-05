package hdfs

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

type Job struct {
	Name         string
	Input        []string
	Output       string
	Mapper       string
	Reducer      string
	Combiner     string
	ReducerTasks int
	Properties   map[string]string // -D key=value
	CacheFiles   []string          // -files
	Files        []string          // -file
}

func (j Job) JarArgs() (args []string) {
	for _, f := range j.Input {
		args = append(args, "-input", hdfsFile(f).String())
	}
	args = append(args, "-output", hdfsFile(j.Output).String())
	args = append(args, "-mapper", j.Mapper)
	if j.Combiner != "" {
		args = append(args, "-combiner", j.Combiner)
	}
	args = append(args, "-reducer", j.Reducer)
	return
}

// PropertyArgs returns the '-D key=value' arguments for hadoop-streaming.jar
func (j Job) PropertyArgs() (args []string) {
	if _, ok := j.Properties["mapred.job.name"]; !ok {
		args = append(args, "-D", fmt.Sprintf("mapred.job.name=%s", j.Name))
	}
	if _, ok := j.Properties["mapred.reduce.tasks"]; !ok {
		args = append(args, "-D", fmt.Sprintf("mapred.reduce.tasks=%d", j.ReducerTasks))
	}
	for k, v := range j.Properties {
		args = append(args, "-D", fmt.Sprintf("%s=%s", k, v))
	}
	return
}

func SubmitJob(j Job) error {
	// http://hadoop.apache.org/docs/r1.1.1/streaming.html
	// https://hadoop.apache.org/docs/r2.9.0/hadoop-streaming/HadoopStreaming.html
	if j.Mapper == "" || j.Reducer == "" {
		return errors.New("missing argument Mapper or Reducer")
	}
	jar, err := StreamingJar()
	if err != nil {
		log.Printf("failed finding streaming jar %s", err)
		return err
	}

	args := []string{"jar", jar}
	args = append(args, j.PropertyArgs()...)

	// -cmdenv name=value	// Pass env var to streaming commands

	// -files is a generic option; must come before streaming options
	if len(j.CacheFiles) > 0 {
		var s []string
		for _, f := range j.CacheFiles {
			s = append(s, hdfsFile(f).String())
		}
		args = append(args, "-files", strings.Join(s, ","))
	}

	for _, f := range j.Files {
		args = append(args, "-file", f)
	}
	args = append(args, j.JarArgs()...)
	cmd := exec.Command(hadoopBinPath("hadoop"), args...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
