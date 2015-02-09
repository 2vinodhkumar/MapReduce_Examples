package IOFormatAndInpustSplits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool {

	public static void main(String[] args)
	{
		try {
			ToolRunner.run(new Driver(), args);
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
		
		Job job=new Job(cfg,"SecondarySort");
		job.setJarByClass(Driver.class);
		job.setMapperClass(SSMapper.class);
		
		job.setMapOutputKeyClass(Employee.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setPartitionerClass(SSPartitioner.class);
		
		job.setReducerClass(SSReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
		return 0;
	}

	
}
