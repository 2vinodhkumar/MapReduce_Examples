package IOFormatAndInputSplits.SplitPreventing.Sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

	public static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
		private Text filename;
		@Override
		public void setup(Context context)
		{
			InputSplit split=context.getInputSplit();
			Path path=((FileSplit)split).getPath();
			filename=new Text(path.toString());
			System.out.println(filename);
		}
		@Override
		public void map(NullWritable key,BytesWritable content,Context context) throws IOException, InterruptedException
		{
			context.write(filename, content);
		}
	}
	
	public int run(String[] args){
		try{
			Configuration cfg=new Configuration();
			cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
			
			Job job=new Job(cfg,"SequenceFile");
			
			job.setJarByClass(SmallFilesToSequenceFileConverter.class);
			job.setMapperClass(SequenceFileMapper.class);
			
			job.setInputFormatClass(WholeFileInputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(BytesWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
	}
}
