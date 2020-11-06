package part1

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class PageMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val line = value.toString
    val words = line.split("\\s+").filter(_.length > 0)

    try
    {
      if ((words(0).equalsIgnoreCase("en")) || (words(0).equalsIgnoreCase("en.m")))
        context.write(new Text(words(1)), new IntWritable(words(2).toInt))
    }
  }
}
