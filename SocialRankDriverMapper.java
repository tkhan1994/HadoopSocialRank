import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialRankDriverMapper extends Mapper<Object, Text, Text, Text>
{
      private Text current_user = new Text();
      private Text current_friend = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
    	  //For each key value pair emit the key and the values
    	  String line = value.toString();
    	  String[] tokens = line.split("\\s"); 
    	  current_user.set(new Text(tokens[0]));
    	  current_friend.set(new Text(tokens[1]));
    	  context.write(current_user, current_friend);
      }
}
