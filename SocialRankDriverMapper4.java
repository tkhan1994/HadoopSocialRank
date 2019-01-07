import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialRankDriverMapper4 extends Mapper<Object, Text, DoubleWritable, IntWritable>
{
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
    	  IntWritable user = new IntWritable();
    	  DoubleWritable val = new DoubleWritable();
    	  String line = value.toString();
    	  String[] tokens = line.split("\\|");
    	  
    	  user.set(Integer.parseInt(tokens[0]));
    	  val.set(Double.parseDouble(tokens[3]));
    	  
    	  context.write(val, user);
      }
}
