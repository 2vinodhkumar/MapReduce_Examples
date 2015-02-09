package sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LookupRecordByKey extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration cfg=getConf();
		JobConf conf=new JobConf(cfg);
		conf.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		Path mapFile=new Path("/user/vinodh/sortdataoutput/mapfiles/part-00000/");
		FileSystem fs=mapFile.getFileSystem(conf);
		IntWritable lookupKey=new IntWritable(223);
		
		Reader[] reader=MapFileOutputFormat.getReaders(fs,mapFile,conf);
		Partitioner<IntWritable,Text> partitioner=new HashPartitioner<IntWritable,Text>();
		
		Text val=new Text();
		
		Writable entry=MapFileOutputFormat.getEntry(reader, partitioner, lookupKey, val);
		System.out.println(lookupKey.get()+"\t"+val.toString());
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new LookupRecordByKey(), args);
	}

}
