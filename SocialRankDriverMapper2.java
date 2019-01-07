import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialRankDriverMapper2 extends Mapper<Object, Text, Text, Text>
{
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
    	  String line = value.toString();
    	  String[] tokens = line.split("\\|");
    	  String current_user = tokens[0];
    	  String his_friends = tokens[2];
    	  String current_rank = tokens[3];
    	  
    	  String append_his_frnds = "[";
    	  append_his_frnds += his_friends;
    	  append_his_frnds += "]";
    	  context.write(new Text(current_user), new Text(append_his_frnds));	// Output 1 ----> [2,3]
    	  
    	  String[] friends = his_friends.split("\\,");
    	  if(!his_friends.equals(""))
    	  {
    		  Double emit_rank = Double.parseDouble(current_rank)/friends.length;
    		  for(int i = 0; i < friends.length; i++)
    		  {
    			  //Output friend ---> current rank/number of friends (helps later)
    			  context.write(new Text(friends[i]), new Text(emit_rank.toString()));
    		  }    	  
    	  }
      }
}