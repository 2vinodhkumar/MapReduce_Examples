package sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqFileToMapFileBySortingKeys extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=getConf();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		
		
		JobConf conf=new JobConf(cfg);
//		conf.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(conf, new Path(args[0]));
		MapFileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient client=new JobClient(conf);
		client.submitJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SeqFileToMapFileBySortingKeys(), args);
	}
}
