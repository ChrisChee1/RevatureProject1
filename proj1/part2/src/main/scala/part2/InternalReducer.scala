package part2

import java.lang

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class InternalReducer extends Reducer[Text, LongWritable, Text, LongWritable] {

  override def reduce(key: Text, values: lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {

    var count = 0

    values.forEach(count += _.get().toInt)

    context.write(key, new LongWritable(count))
  }
}
