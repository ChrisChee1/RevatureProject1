package part2

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class InternalMapper extends Mapper[LongWritable, Text, Text, LongWritable] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {

    val fields = value.toString.split("\\s+").filter(_.length > 0)

    if (fields(2).equalsIgnoreCase("link"))
      context.write(new Text(fields(0)), new LongWritable(fields(3).toLong))
  }
}
