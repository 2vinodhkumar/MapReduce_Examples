package IOFormatAndInputSplits.SplitPreventing.Sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverToWriteWholeFileDATA extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		try{
			Configuration cfg =new Configuration();
			cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
			cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));
			Job job=new Job(cfg,"CustomSplits");
			job.setJarByClass(DriverToWriteWholeFileDATA.class);
			job.setMapperClass(WholeFileDATAOutput.class);

			
			job.setInputFormatClass(WholeFileInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(0);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
			int exitcode=ToolRunner.run(new DriverToWriteWholeFileDATA(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
