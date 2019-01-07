import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialRankDriverReducer2 extends Reducer<Text, Text, Text, Text>
{
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
    	  String str_key = key.toString();
    	  str_key += "|";
    	  Double ranks_sum = 0.0;
    	  String his_friends = "|";
    	  while(values.iterator().hasNext()) 
    	  {
    		  String current_value = values.iterator().next().toString();
    		  if(Character.toString(current_value.charAt(0)).equals("["))
    		  {
    			  current_value = current_value.substring(1, current_value.length() - 1); // Remove the brackets
    			  his_friends += current_value;
    		  }
    		  else
    		  {
    			  ranks_sum += Double.parseDouble(current_value);
    		  }
    	  }
		  his_friends += "|";
    	  ranks_sum = (ranks_sum * 0.85) + 0.15;
    	  his_friends += ranks_sum.toString();
    	  context.write(new Text(str_key), new Text(his_friends));
      }
}