package IOFormatAndInputSplits.SplitPreventing.Sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadOutputofSequenceFile extends Configured implements Tool{

	public static class MapperToRead extends Mapper<NullWritable,BytesWritable,NullWritable,Text>
	{
		public static Text da=new Text();
		public  void map(NullWritable key,BytesWritable value,Context context) throws IOException, InterruptedException
		{
			String data=new String(value.getBytes());
			da.set(data);
			System.out.println(data);
			context.write(key, da);
			
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new ReadOutputofSequenceFile(),args);
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=new Configuration();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		Job job=new Job(cfg,"ReadSeq");
		
		job.setJarByClass(ReadOutputofSequenceFile.class);
		job.setMapperClass(MapperToRead.class);
		job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

}
