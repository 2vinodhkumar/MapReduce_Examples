import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AggregateMapper extends Mapper<Text, Text, Text, Text> {
//	public class AggregateMapper extends Mapper<IntWritable, Text, Text, Text> {
	
	public Text word=new Text();
	
	public Text keyText=new Text();
	
	public void map(Text key,Text value,Context context) throws IOException, InterruptedException
	{
//		public void map(IntWritable key,Text value,Context context) throws IOException, InterruptedException
//		{
		StringTokenizer str=new StringTokenizer(value.toString(),",");
		while(str.hasMoreTokens())
		{
//			int i=key.get();
			word.set(str.nextToken());
//			keyText.set(Integer.toString(i));
//			context.write(keyText, word);
			context.write(key, word);
		}
	}

}
