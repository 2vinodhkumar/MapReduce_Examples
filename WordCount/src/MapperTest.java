
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MapperTest {

	@SuppressWarnings("deprecation")
	@org.junit.Test
	public void testMap() throws IOException
	{
		Text val=new Text("Hi,hello"+'\n'+"hello unit test");
		MapDriver<LongWritable, Text, Text, IntWritable> md=new MapDriver<LongWritable,Text,Text,IntWritable>();
		md.withMapper(new WordCountMapper());
		md.setInput(new LongWritable(20), new Text("Hi"));
		md.setInput(new LongWritable(24), new Text("Hi"));
		md.setInput(new LongWritable(27), new Text("Hi"));
		
		md.withOutput(new Text("Hi"), new IntWritable(1));
		md.runTest();
		
		
	}
}
