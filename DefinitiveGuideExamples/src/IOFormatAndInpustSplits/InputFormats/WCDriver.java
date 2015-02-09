package IOFormatAndInpustSplits.InputFormats;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCDriver extends Configured implements Tool {

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
Configuration cfg=new Configuration();
		
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(cfg);
		
		
		try {
			//JobConfiguration
			JobConf conf=new JobConf(cfg,WCDriver.class);
			
			Job job=new Job(cfg,"WordCount");
		
			job.setJarByClass(WCDriver.class);
			job.setMapperClass(WordCountMapper.class);
			
			//RducerClass
			//To Produce Sorted Map output without Reducer Phase comment below
			job.setReducerClass(WordCountReducer.class);
			
			
			// TODO: specify output types
			job.setInputFormatClass(CombineFileInputFormatConc.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			//Output of Reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			
			//This will execute Reduce task with 0 tasks, from Map task unsorted output will be produced
			job.setNumReduceTasks(1);
		
			//File to be read
//			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPaths(job, args[0]+","+"/user/vinodh/wcoinput/out/_logs/history,/user/vinodh/maxtemp/input/227070-99999-1905");
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
//			FileInputFormat.setInputPathFilter(job, ExcludeDIRFilter.class);
		
			job.waitForCompletion(true);
		
	}catch(Exception e)
	{
		e.printStackTrace();
	}
		return 0;
	}



	public static void main(String[] args)
	{
		try {
			int exitCode = ToolRunner.run(new WCDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}
}
