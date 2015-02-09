package sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortDataPreprocessor extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=getConf();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		Job job=new Job(cfg,"Sort");
		
		job.setJarByClass(SortDataPreprocessor.class);
		job.setMapperClass(CleanerMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

	static class CleanerMapper extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		IntWritable outk=new IntWritable();
		public void map(LongWritable key,Text val,Context context) throws IOException, InterruptedException
		{
			String data =val.toString();
			String da=data.substring(0, 3);  //data is like "122" "some matter outputting first 4 chars as key
			outk.set(Integer.parseInt(da));
			context.write(outk, val);
			
		}
	}
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SortDataPreprocessor(), args);
	}
}
