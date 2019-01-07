import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialRankDriverReducer3 extends Reducer<Text, Text, Text, Text>
{
	  Double max_diff = 0.0;
	  String max_vertex = "";
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
    	  ArrayList<String> vals = new ArrayList<String>();
    	  while(values.iterator().hasNext())
    	  {
    		  Text t = values.iterator().next();
    		  vals.add(t.toString());
    	  }
    	  Double value_to_use = 1.0;
    	  Double diff = 0.0;
    	  if(vals.size() == 2)
    	  {
    		  value_to_use = Double.parseDouble(vals.get(1));
    	  }
    	  diff = value_to_use - Double.parseDouble(vals.get(0));
    	  diff = Math.abs(diff);
    	  if(diff > max_diff)
    	  {
    		  max_diff = diff;
    		  max_vertex = key.toString();
    	  }
      }
      
      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
          context.write(new Text(max_vertex), new Text(max_diff.toString()));
      }
      
}