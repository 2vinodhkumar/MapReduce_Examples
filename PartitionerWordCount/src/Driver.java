import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		try {
			int exitCode = ToolRunner.run(new Driver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=new Configuration();
		
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(cfg);
		System.out.println(fs.getHomeDirectory());
		
		
		try {
			//JobConfiguration
			JobConf conf=new JobConf(cfg,Driver.class);
			Job job=new Job(cfg,"WordCount");
			job.setJarByClass(Driver.class);
			job.setMapperClass(WordCountMapper.class);
			
			//RducerClass
			//To Produce Sorted Map output without Reducer Phase comment below
			job.setReducerClass(WordCountReducer.class);
			
			
			// TODO: specify output types
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			//Output of Reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setPartitionerClass(WordPartitioner.class);
			
			//This will execute Reduce task with 0 tasks, from Map task unsorted output will be produced
			job.setNumReduceTasks(2);
		
			//File to be read
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		
	}catch(Exception e)
	{
		e.printStackTrace();
	}
		return 0;
	}
}
