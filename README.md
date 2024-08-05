# GoMRJob 

![Build Status](https://github.com/jehiah/gomrjob/actions/workflows/ci.yaml/badge.svg?branch=master)
[![Docs](https://pkg.go.dev/badge/github.com/jehiah/gomrjob.svg)](http://pkg.go.dev/github.com/jehiah/gomrjob)
[![GitHub release](https://img.shields.io/github/release/jehiah/gomrjob.svg)](https://github.com/jehiah/gomrjob/releases/latest)


A Go framework for running Map Reduce Jobs on Hadoop.

http://godoc.org/github.com/jehiah/gomrjob

### Supported Configurations

* Hadoop with HDFS via `hadoop` CLI
* [Google Cloud Dataproc](https://cloud.google.com/dataproc/) with [Google Storage](https://cloud.google.com/storage/)

### About

This framework has been in production use at [Bitly](https://bitly.com/) since 2013, but it's light on examples. 

See the [example](./example) for more context.

Heavily inspired by [`Yelp/mrjob`](https://github.com/Yelp/mrjob)