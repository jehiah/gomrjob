package gomrjob

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

func HasHadoop() bool {
	hadoopHome := os.Getenv("HADOOP_HOME")
	return hadoopHome != ""
}

func hadoopBinPath(tool string) string {
	hadoopHome := os.Getenv("HADOOP_HOME")
	return path.Join(hadoopHome, "bin", tool)
}

var streamingJarPath string

// the hadoop streaming jar (hadoop*streaming*.jar) is searched for
// under the $HADOOP_HOME path, or is set via the $HADOOP_STREAMING_JAR
// environment variable.
func StreamingJar() (string, error) {
	if streamingJarPath != "" {
		return streamingJarPath, nil
	}
	streamingJarPath = os.Getenv("HADOOP_STREAMING_JAR")
	if streamingJarPath != "" {
		return streamingJarPath, nil
	}
	hadoopHome := os.Getenv("HADOOP_HOME")
	if hadoopHome == "" {
		return "", errors.New("env HADOOP_HOME not set")
	}
	p := regexp.MustCompile("^hadoop.*streaming.*\\.jar$")
	w := func(pathString string, info os.FileInfo, err error) error {
		if p.FindString(path.Base(pathString)) != "" {
			streamingJarPath = pathString
			return errors.New("found streaming jar")
		}
		return nil
	}
	filepath.Walk(hadoopHome, w)
	if streamingJarPath == "" {
		return "", errors.New("no streaming.jar found")
	}
	return streamingJarPath, nil
}

// http://hadoop.apache.org/docs/r0.20.2/hdfs_shell.html

func Mkdir(remote string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), "fs", "-mkdir", remote)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// http://hadoop.apache.org/docs/r1.1.1/file_system_shell.html#test
// flag is 
// -e check to see if the file exists. Return 0 if true. 
// -z check to see if the file is zero length. Return 0 if true. 
// -d check to see if the path is directory. Return 0 if true. 
func Test(flag string, remote string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), "fs", "-test", flag, remote)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func Put(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-put"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func RMR(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-rmr"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func PutStream(args ...string) *exec.Cmd {
	if len(args) < 1 || args[0] != "-" {
		args = append([]string{"-"}, args...) // prepend w/ stdin flag
	}
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-put"}, args...)...)
	log.Print(cmd.Args)
	return cmd
}

func Copy(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-cp"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func Cat(args ...string) *exec.Cmd {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-cat"}, args...)...)
	log.Print(cmd.Args)
	return cmd
}

type hdfsFile struct {
	path string
}

func (f hdfsFile) String() string {
	if strings.HasPrefix(f.path, "hdfs://") {
		return f.path
	}
	return fmt.Sprintf("hdfs://%s", f.path)
}

type Job struct {
	Name         string
	Input        []string
	Output       string
	Mapper       string
	Reducer      string
	Combiner     string
	Options      []string
	ReducerTasks int
	CacheFiles   []string
}

func SubmitJob(j Job) error {
	// http://hadoop.apache.org/docs/r0.20.2/streaming.html
	// http://hadoop.apache.org/docs/r1.1.1/streaming.html
	jar, err := StreamingJar()
	if err != nil {
		log.Printf("failed finding streaming jar %s", err)
		return err
	}

	args := []string{"jar", jar}
	if len(j.Options) > 0 {
		args = append(args, j.Options...)
	}
	args = append(args, "-D", fmt.Sprintf("mapred.job.name=%s", j.Name))
	// -D mapred.map.tasks=1
	args = append(args, "-D", fmt.Sprintf("mapred.reduce.tasks=%d", j.ReducerTasks))
	// -cmdenv name=value	// Pass env var to streaming commands

	for _, f := range j.Input {
		args = append(args, "-input", hdfsFile{f}.String())
	}
	for _, f := range j.CacheFiles {
		args = append(args, "-cacheFile", hdfsFile{f}.String())
		// -file? --files?
	}
	args = append(args, "-output", hdfsFile{j.Output}.String())
	if j.Mapper != "" {
		args = append(args, "-mapper", j.Mapper)
	}
	if j.Combiner != "" {
		args = append(args, "-combiner", j.Combiner)
	}
	if j.Reducer != "" {
		args = append(args, "-reducer", j.Reducer)
	}
	cmd := exec.Command(hadoopBinPath("hadoop"), args...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
