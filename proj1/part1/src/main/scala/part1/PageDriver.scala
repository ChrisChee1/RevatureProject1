package part1

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object PageDriver extends App {

  if (args.length != 2) {
    println("Usage: PageDriver <input dir> <output dir>")
    System.exit(-1)
  }

  val job = Job.getInstance()

  job.setJarByClass(PageDriver.getClass)

  job.setJobName("Page View Count")

  FileInputFormat.setInputPaths(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))

  job.setMapperClass(classOf[PageMapper])
  job.setReducerClass(classOf[PageReducer])

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])

  val success = job.waitForCompletion(true): Boolean
  System.exit(if (success) 0 else 1)

}