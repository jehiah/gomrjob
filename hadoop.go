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

func Copy(args ...string) error {
	cmd := exec.Command(hadoopBinPath("hadoop"), append([]string{"fs", "-cp"}, args...)...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func SubmitJob(name string, input string, output string, processName string) error {
	jar, err := StreamingJar()
	if err != nil {
		log.Printf("failed finding streaming jar %s", err)
		return err
	}

	args := []string{"jar", jar}
	args = append(args, "-D", fmt.Sprintf("mapred.job.name=%s", name))
	args = append(args, "-input", input)
	args = append(args, "-output", output)
	args = append(args, "-cacheFile", fmt.Sprintf("%s#%s", processName, processName))
	args = append(args, "-mapper", fmt.Sprintf("%s --step=map", processName))
	// -combiner
	// numReduceTasks
	args = append(args, "-reducer", fmt.Sprintf("%s --step=reduce", processName))
	cmd := exec.Command(hadoopBinPath("hadoop"), args...)
	log.Print(cmd.Args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
