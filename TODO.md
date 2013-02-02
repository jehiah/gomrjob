
* sample input files for local trial run (non hadoop mode)
* multi-step jobs
    * chain input/output
    * skip map/reduce on some steps (ie: 2nd reduce phase)
* prefix step/stage for error logs
* limit amount of error logs written
* support inline combiner [before serializing]
* finish protocol tests
* test hadoop commands
* access to the name of the input file (for mapper. reducer too?)
* batch reporter counting with a timer
* csv/msql dump input protocols
* mapreduce.task.timeout: 3600000
* compressed output files
    * -D mapred.output.compress=true 
    * -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCode
