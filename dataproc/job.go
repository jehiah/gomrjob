package dataproc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/jehiah/gomrjob/hdfs"
)

func isTerminalState(s string) bool {
	switch s {
	case "ATTEMPT_FAILURE", "ERROR", "DONE", "CANCELLED":
		return true
	default:
		return false
	}
}

// https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.jobs/submit
type jobRequest struct {
	RequestID string `json:"requestId,omitempty"`
	Job       job    `json:"job"`
}
type job struct {
	Placement struct {
		ClusterName string `json:"cluster_name"`
	} `json:"placement"`
	Reference struct {
		JobID string `json:"jobId,omitempty"`
	} `json:"reference,omitempty"`
	// https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/HadoopJob
	HadoopJob struct {
		Args           []string          `json:"args"`
		MainJarFileURI string            `json:"mainJarFileUri"`
		FileURIs       []string          `json:"fileUris,omitempty"`
		Properties     map[string]string `json:"properties,omitempty"`
	} `json:"hadoopJob"`
	Status struct {
		State          string `json:"state,omitempty"`
		StateStartTime string `json:"stateStartTime,omitempty"`
		Details        string `json:"details,omitempty"`
		SubState       string `json:"substate,omitempty"`
	} `json:"status,omitempty"`
}

func SubmitJob(j hdfs.Job, client *http.Client, project, region, cluster string) error {
	if j.Mapper == "" || j.Reducer == "" {
		return errors.New("missing argument Mapper or Reducer")
	}
	p := make(map[string]string, len(j.Properties))
	for k, v := range j.Properties {
		p[k] = v
	}
	if _, ok := p["mapred.job.name"]; !ok {
		p["mapred.job.name"] = j.Name
	}
	if _, ok := p["mapred.reduce.tasks"]; !ok {
		p["mapred.reduce.tasks"] = fmt.Sprintf("%d", j.ReducerTasks)
	}

	var req jobRequest
	req.Job.Reference.JobID = j.Name
	req.Job.Placement.ClusterName = cluster
	req.Job.HadoopJob.MainJarFileURI = "file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar"
	req.Job.HadoopJob.Args = j.JarArgs()
	req.Job.HadoopJob.FileURIs = j.CacheFiles
	req.Job.HadoopJob.Properties = p

	resource := fmt.Sprintf("https://dataproc.googleapis.com/v1/projects/%s/regions/%s/jobs:submit", url.PathEscape(project), url.PathEscape(region))
	job, err := post(client, resource, req)
	if err != nil {
		return err
	}
	state := job.Status.State
	log.Printf("job:%s status:%s", job.Reference.JobID, state)

	resource = fmt.Sprintf("https://dataproc.googleapis.com/v1/projects/%s/regions/%s/jobs/%s", url.PathEscape(project), url.PathEscape(region), url.PathEscape(job.Reference.JobID))
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	var i int
	for range ticker.C {
		i++
		job, err = get(client, resource)
		if err != nil {
			return err
		}
		// if state changes or 30s passes by
		if state != job.Status.State || i%15 == 0 {
			state = job.Status.State
			log.Printf("job:%s status:%s", job.Reference.JobID, state)
		}
		if isTerminalState(state) {
			break
		}
	}
	return nil
}

func get(client *http.Client, resource string) (*job, error) {
	resp, err := client.Get(resource)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		log.Print(string(respBody))
		return nil, fmt.Errorf("got status code %d", resp.StatusCode)
	}
	var j job
	return &j, json.Unmarshal(respBody, &j)
}

func post(client *http.Client, resource string, req jobRequest) (*job, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	log.Printf("Submitting job %q to Dataproc cluster %q", req.Job.Reference.JobID, req.Job.Placement.ClusterName)
	log.Print(resource)
	log.Printf("args: %q", req.Job.HadoopJob.Args)
	for k, v := range req.Job.HadoopJob.Properties {
		log.Printf("   -D %s=%v", k, v)
	}
	resp, err := client.Post(resource, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		log.Print(string(respBody))
		return nil, fmt.Errorf("got status code %d", resp.StatusCode)
	}
	var j job
	return &j, json.Unmarshal(respBody, &j)
}
