import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialRankDriverReducer4 extends Reducer<DoubleWritable, IntWritable, Text, Text>
{
      public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
      {
    	  ArrayList<String> vals = new ArrayList<String>();
    	  while(values.iterator().hasNext())
    	  {
        	  while(values.iterator().hasNext())
        	  {
        		  IntWritable user = values.iterator().next();
        		  vals.add(user.toString());
        	  }
    	  }
    	  for(int i = 0; i < vals.size(); i++)
    	  {
    		  context.write(new Text(vals.get(i)),new Text(key.toString()));  
    	  }
      }
}