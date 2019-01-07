import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SocialRankDriver 
{
	public static void main(String[] args) throws Exception 
	{
		if (args[0].toString().equals("init"))
		{
			//-----Job 1-----//
	        Job job = Job.getInstance(new Configuration());
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        job.setJarByClass(SocialRankDriver.class);
	        job.setMapperClass(SocialRankDriverMapper.class); 
	        job.setReducerClass(SocialRankDriverReducer.class);
	        
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        
	        
	        if(fs.exists(new Path(args[2])))
	        {
	        	fs.delete(new Path(args[2]),true);
	        }
	        
	        FileInputFormat.setInputPaths(job, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job, new Path(args[2]));
	        
	        job.setNumReduceTasks(Integer.parseInt(args[3]));
	        job.submit();
	        job.waitForCompletion(true);
		}
		else if(args[0].toString().equals("iter"))
		{
			//-----Job 2-----//
			
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
			
			Job job2 = Job.getInstance(new Configuration());
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(Text.class);
	        job2.setJarByClass(SocialRankDriver.class);
	        job2.setMapperClass(SocialRankDriverMapper2.class); 
	        job2.setReducerClass(SocialRankDriverReducer2.class);
	        
	        if(fs.exists(new Path(args[2])))
	        {
	        	fs.delete(new Path(args[2]), true);
	        }
	        
	        
	        FileInputFormat.setInputPaths(job2, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	        
	        job2.setNumReduceTasks(Integer.parseInt(args[3]));
	        job2.submit();
	        job2.waitForCompletion(true);
		}
		else if(args[0].toString().equals("diff"))
		{
			//-----Job3-----//
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
			
			Job job3 = Job.getInstance(new Configuration());
	        job3.setOutputKeyClass(Text.class);
	        job3.setOutputValueClass(Text.class);
	        job3.setJarByClass(SocialRankDriver.class);
	        job3.setMapperClass(SocialRankDriverMapper3.class); 
	        job3.setReducerClass(SocialRankDriverReducer3.class);
	        
	        if(fs.exists(new Path(args[3])))
	        {
	        	fs.delete(new Path(args[3]), true);
	        }
	        
	        FileInputFormat.setInputPaths(job3, new Path(args[1]), new Path(args[2]));
	        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
	        
	        job3.setNumReduceTasks(Integer.parseInt(args[4]));
	        job3.submit();
	        job3.waitForCompletion(true);
		}
		else if(args[0].toString().equals("finish"))
		{
			//-----Job4-----//
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
			
			Job job4 = Job.getInstance(new Configuration());
			job4.setMapOutputKeyClass(DoubleWritable.class);
			job4.setMapOutputValueClass(IntWritable.class);
	        job4.setOutputKeyClass(Text.class);
	        job4.setOutputValueClass(Text.class);
	        job4.setJarByClass(SocialRankDriver.class);
	        job4.setMapperClass(SocialRankDriverMapper4.class); 
	        job4.setReducerClass(SocialRankDriverReducer4.class);
	        
	        if(fs.exists(new Path(args[2])))
	        {
	        	fs.delete(new Path(args[2]), true);
	        }
	        
	        FileInputFormat.setInputPaths(job4, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job4, new Path(args[2]));
	        
	        job4.setSortComparatorClass(SortIntComparator.class);
	        job4.setNumReduceTasks(Integer.parseInt(args[3]));
	        job4.submit();
	        job4.waitForCompletion(true);
		}
		else if(args[0].toString().equals("composite"))
		{
			
			 Configuration conf = new Configuration();
		     FileSystem fs = FileSystem.get(conf);
			//-----------------Initial Format-----------------------//
			Job job = Job.getInstance(new Configuration());
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        job.setJarByClass(SocialRankDriver.class);
	        job.setMapperClass(SocialRankDriverMapper.class); 
	        job.setReducerClass(SocialRankDriverReducer.class);
	        if(fs.exists(new Path(args[3])))
	        {
	        	fs.delete(new Path(args[3]),true);	//Interim 1 Directory
	        }
	        FileInputFormat.setInputPaths(job, new Path(args[1]));	//Input Directory
	        FileOutputFormat.setOutputPath(job, new Path(args[3]));
	        job.setNumReduceTasks(Integer.parseInt(args[6]));
	        job.submit();
	        job.waitForCompletion(true);
			//---------------------Iterations----------------------//
	        Integer count = 0;
	        Integer switch_outs = 0;
	        Integer val1 = 0;
	        Integer val2 = 0;
	        while(true)
	        {
				Job job2 = Job.getInstance(new Configuration());
		        job2.setOutputKeyClass(Text.class);
		        job2.setOutputValueClass(Text.class);
		        job2.setJarByClass(SocialRankDriver.class);
		        job2.setMapperClass(SocialRankDriverMapper2.class); 
		        job2.setReducerClass(SocialRankDriverReducer2.class);
		        
		        if(switch_outs % 2 == 0)
		        {
		        	val1 = 3;
		        	val2 = 4;
		        }
		        else
		        {
		        	val1 = 4;
		        	val2 = 3;
		        }
		        switch_outs += 1;
		        switch_outs = switch_outs % 2;
		        if(fs.exists(new Path(args[val2])))
		        {
		        	fs.delete(new Path(args[val2]), true);
		        }
		        FileInputFormat.setInputPaths(job2, new Path(args[val1]));
		        FileOutputFormat.setOutputPath(job2, new Path(args[val2]));
		        job2.setNumReduceTasks(Integer.parseInt(args[6]));
		        job2.submit();
		        job2.waitForCompletion(true);
	        	
	        	if(count % 3 == 0)	//Run difference loop after intervals
	        	{
	     			Job job3 = Job.getInstance(new Configuration());
	     	        job3.setOutputKeyClass(Text.class);
	     	        job3.setOutputValueClass(Text.class);
	     	        job3.setJarByClass(SocialRankDriver.class);
	     	        job3.setMapperClass(SocialRankDriverMapper3.class); 
	     	        job3.setReducerClass(SocialRankDriverReducer3.class);
	     	        if(fs.exists(new Path(args[5])))
	     	        {
	     	        	fs.delete(new Path(args[5]), true);
	     	        }
	     	        FileInputFormat.setInputPaths(job3, new Path(args[3]), new Path(args[4]));
	     	        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
	     	        job3.setNumReduceTasks(Integer.parseInt(args[6]));
	     	        job3.submit();
	     	        job3.waitForCompletion(true);
	     	        
	     	        count = count % 3;
	        	}
	        	count += 1;
	        	if(fs.exists(new Path(args[5])))
	        	{
	        		String file_path = args[5].toString() + "/part-r-00000";
	        		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file_path))));
	        		String line = br.readLine();
	        		String[] tokens = line.split("\\s");
	        		Double value = Double.parseDouble(tokens[1]);
	        		if(value < 0.001)
	        		{
	        			break;
	        		}
	        	}
	        }
	        //--------------------FINISH----------------------//
	        Job job4 = Job.getInstance(new Configuration());
			job4.setMapOutputKeyClass(DoubleWritable.class);
			job4.setMapOutputValueClass(IntWritable.class);
	        job4.setOutputKeyClass(Text.class);
	        job4.setOutputValueClass(Text.class);
	        job4.setJarByClass(SocialRankDriver.class);
	        job4.setMapperClass(SocialRankDriverMapper4.class); 
	        job4.setReducerClass(SocialRankDriverReducer4.class);
	        if(fs.exists(new Path(args[2])))
	        {
	        	fs.delete(new Path(args[2]), true);
	        }
	        FileInputFormat.setInputPaths(job4, new Path(args[val2]));
	        FileOutputFormat.setOutputPath(job4, new Path(args[2]));
	        job4.setSortComparatorClass(SortIntComparator.class);
	        job4.setNumReduceTasks(Integer.parseInt(args[6]));
	        job4.submit();
	        job4.waitForCompletion(true);
		}
	}   
}