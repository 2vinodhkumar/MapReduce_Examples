package Counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageCounterDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=getConf();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/mapred-site.xml"));
		Job job=new Job(cfg,"ImageCounter");
		job.setJarByClass(ImageCounterDriver.class);
		job.setMapperClass(ImageCounterMapper.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	long jpgFiles=	job.getCounters().findCounter(ImageCounter.type.JPG).getValue();
	long gifFiles=job.getCounters().findCounter(ImageCounter.type.GIF).getValue();
	long others=job.getCounters().findCounter(ImageCounter.type.OTHERS).getValue();
	
	System.out.println(jpgFiles+"\t"+gifFiles+"\t"+others);
			
	return 0;
	}
	public static void main(String[] args) 
	{
		
		try {
		int	x = ToolRunner.run(new ImageCounterDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
