import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static IntWritable one=new IntWritable(1);
	
	public Text word =new Text();
	
	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException
	{
		String line=value.toString();
		StringTokenizer stringTokens=new StringTokenizer(line);
		while(stringTokens.hasMoreTokens())
		{
			word.set(stringTokens.nextToken());
			context.write(word, one);
		}
	}

}
