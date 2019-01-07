import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialRankDriverReducer extends Reducer<Text, Text, Text, Text>
{
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
    	  // This will give the output which will tell us the friends of the user
    	  // And the current rank. This is also the intermediate form
    	  //e.g the output will be	1 ---> 2,3	1
    	  String output = "|";
    	  String str_key = key.toString();
    	  while(values.iterator().hasNext())
    	  {
    		  Text user = new Text(values.iterator().next());
    		  output += user.toString();
    		  output += ",";
    	  }
    	  output = output.substring(0, output.length() - 1);
    	  output += "|";
    	  output += "1";	//Append the initial rank
    	  str_key += "|";
    	  context.write(new Text(str_key), new Text(output));
      }
}