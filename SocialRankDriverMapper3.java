import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialRankDriverMapper3 extends Mapper<Object, Text, Text, Text>
{
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
    	  String line = value.toString();
    	  String[] tokens = line.split("\\|");
    	  context.write(new Text(tokens[0]), new Text(tokens[3]));
      }
}