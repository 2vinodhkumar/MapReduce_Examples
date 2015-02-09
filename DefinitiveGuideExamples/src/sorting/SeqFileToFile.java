package sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqFileToFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=getConf();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		Job job=new Job(cfg,"Read");
		
		job.setJarByClass(SeqFileToFile.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(SeqToFile.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}
	public static class SeqToFile extends Mapper<IntWritable,Text,NullWritable,Text>
	{
		public void map(IntWritable key,Text val,Context context) throws IOException, InterruptedException
		{
//			context.write(NullWritable.get(), val);
			System.out.println(key.get()+"\t"+val.toString());
		}
	}
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SeqFileToFile(), args);
	}

}
