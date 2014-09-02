package hdfs

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
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

func FsCmd(command string, args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", command}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func Mkdir(remote string) error {
	return FsCmd("-mkdir", remote)
}

// http://hadoop.apache.org/docs/r1.1.1/file_system_shell.html#test
// flag is
// -e check to see if the file exists. Return 0 if true.
// -z check to see if the file is zero length. Return 0 if true.
// -d check to see if the path is directory. Return 0 if true.
func Test(flag string, remote string) error {
	return FsCmd("-test", flag, remote)
}

func Put(args ...string) error {
	return FsCmd("-put", args...)
}

func RMR(args ...string) error {
	return FsCmd("-rmr", args...)
}

func Remove(args ...string) error {
	return FsCmd("-rm", args...)
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
	return FsCmd("-cp", args...)
}

func Move(args ...string) error {
	return FsCmd("-mv", args...)
}

func Cat(args ...string) *exec.Cmd {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-cat"}, args...)...)
	log.Print(cmd.Args)
	return cmd
}

func Ls(args ...string) <-chan *HdfsFile {
	out := make(chan *HdfsFile, 100)
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-ls"}, args...)...)
	rr, _ := cmd.StdoutPipe()
	go func(cmd *exec.Cmd) {
		err := cmd.Run()
		if err != nil {
			log.Printf("ls err %s", err)
		}
	}(cmd)
	go parseLsOutput(rr, out)
	return out
}

func parseLsOutput(in io.Reader, out chan *HdfsFile) {
	var lineErr error
	var line []byte
	r := bufio.NewReader(in)
	for {
		if lineErr == io.EOF {
			break
		} else if lineErr != nil {
			log.Printf("line:%s err: %s", line, lineErr)
			break
		}
		line, lineErr = r.ReadBytes('\n')
		if len(line) <= 1 || bytes.HasPrefix(line, []byte("Found ")) {
			continue
		}
		chunks := splitLsOutput(line)
		file, err := newHdfsFile(chunks)
		if err == nil {
			out <- file
		} else {
			log.Printf("error %s on line %v", err, line)
		}
	}
	close(out)
}

func newHdfsFile(chunks []string) (*HdfsFile, error) {
	// permissions number_of_replicas userid groupid filesize modification_date modification_time filename
	var err error
	if len(chunks) != 8 {
		return nil, errors.New("invalid file parts")
	}
	file := &HdfsFile{}
	// log.Printf("split: %#v", chunks)
	file.ReplicaCount, err = strconv.ParseInt(chunks[1], 10, 64)
	if err != nil {
		return nil, err
	}
	file.User = chunks[2]
	file.Group = chunks[3]
	file.Size, err = strconv.ParseInt(chunks[4], 10, 64)
	if err != nil {
		return nil, err
	}
	file.Modified, err = time.Parse("2006-01-02 15:04", chunks[5]+" "+chunks[6])
	if err != nil {
		return nil, err
	}
	file.Path = chunks[7]
	return file, nil
}

func splitLsOutput(line []byte) []string {
	var o []string
	chunks := bytes.Split(bytes.TrimRight(line, "\n"), []byte(" "))
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		o = append(o, string(chunk))
	}
	return o
}

type HdfsFile struct {
	Permissions  string
	ReplicaCount int64
	User         string
	Group        string
	Size         int64
	Modified     time.Time
	Path         string
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
	Files        []string
}

func SubmitJob(j Job) error {
	// http://hadoop.apache.org/docs/r0.20.2/streaming.html
	// http://hadoop.apache.org/docs/r1.1.1/streaming.html
	if j.Mapper == "" || j.Reducer == "" {
		return errors.New("missing argument Mapper or Reducer")
	}
	jar, err := StreamingJar()
	if err != nil {
		log.Printf("failed finding streaming jar %s", err)
		return err
	}

	args := []string{"jar", jar}
	args = append(args, "-D", fmt.Sprintf("mapred.job.name=%s", j.Name))
	args = append(args, "-D", fmt.Sprintf("mapred.reduce.tasks=%d", j.ReducerTasks))
	if len(j.Options) > 0 {
		args = append(args, j.Options...)
	}
	// -cmdenv name=value	// Pass env var to streaming commands

	for _, f := range j.Input {
		args = append(args, "-input", hdfsFile{f}.String())
	}
	for _, f := range j.CacheFiles {
		args = append(args, "-cacheFile", hdfsFile{f}.String())
		// -file? --files?
	}
	for _, f := range j.Files {
		args = append(args, "-file", f)
	}
	args = append(args, "-output", hdfsFile{j.Output}.String())
	args = append(args, "-mapper", j.Mapper)
	if j.Combiner != "" {
		args = append(args, "-combiner", j.Combiner)
	}
	args = append(args, "-reducer", j.Reducer)
	cmd := exec.Command(hadoopBinPath("hadoop"), args...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
