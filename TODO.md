
* limit amount of logs written
* sample input files for trial run
* structure code handling into steps
* support inline combiner [before serializing]
* mapreduce.task.timeout: 3600000
* compressed output
    mapreduce.output.compress: "true"
    mapreduce.output.compression.codec: org.apache.hadoop.io.compress.GzipCodec
