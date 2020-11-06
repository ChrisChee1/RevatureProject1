package part2

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object InternalDriver extends App {

  if (args.length != 2) {
    println("Usage: InternalDriver <input dir> <output dir>")
    System.exit(-1)
  }

  val job = Job.getInstance()

  job.setJarByClass(InternalDriver.getClass)

  job.setJobName("Internal Links Count")

  FileInputFormat.setInputPaths(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))

  job.setMapperClass(classOf[InternalMapper])
  job.setReducerClass(classOf[InternalReducer])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[LongWritable])

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[LongWritable])

  val success = job.waitForCompletion(true)
  System.exit(if (success) 0 else 1)
}
