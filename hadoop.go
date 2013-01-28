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

func StreamingJar() (string, error) {
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

func Put(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-put"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func Copy(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-cp"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func SubmitJob(name string, input []string, output string, loggerAddress string, processName string) error {
	// http://hadoop.apache.org/docs/r0.20.2/streaming.html
	// http://hadoop.apache.org/docs/r1.1.1/streaming.html
	jar, err := StreamingJar()
	if err != nil {
		log.Printf("failed finding streaming jar %s", err)
		return err
	}

	remoteLogger := ""
	if loggerAddress != "" {
		remoteLogger = fmt.Sprintf(" --remote-logger=%s", loggerAddress)
	}

	args := []string{"jar", jar}
	args = append(args, "-D", fmt.Sprintf("mapred.job.name=%s", name))
	for _, i := range input {
		args = append(args, "-input", fmt.Sprintf("hdfs://%s", i))
	}
	args = append(args, "-output", fmt.Sprintf("hdfs://%s", output))
	args = append(args, "-cacheFile", fmt.Sprintf("hdfs://%s#%s", processName, filepath.Base(processName)))
	args = append(args, "-mapper", fmt.Sprintf("%s --stage=map%s", filepath.Base(processName), remoteLogger))
	if loggerAddress != "" {
		args = append(args)
	}
	// -combiner
	// -D mapred.map.tasks=1
	// -D mapred.reduce.tasks=0".
	args = append(args, "-reducer", fmt.Sprintf("%s --stage=reduce%s", filepath.Base(processName), remoteLogger))
	cmd := exec.Command(hadoopBinPath("hadoop"), args...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
