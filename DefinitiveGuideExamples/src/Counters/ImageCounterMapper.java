package Counters;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

public class ImageCounterMapper extends
		Mapper<Text, Text, Text, IntWritable> {
	
	Counters counters;
/*	public void setup(Context context)
	{
		try {
			super.setup(context);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		counters=context.getCounter(counterName);
	}	*/

	public void map(Text inputKey,Text inputValue,Context context) throws IOException, InterruptedException
	{
		String vals=inputValue.toString();
		if(vals.contains(".JPG"))
		{
			context.getCounter(ImageCounter.type.JPG).increment(1);
		}
		else if(vals.contains(".GIF"))
		{
			context.getCounter(ImageCounter.type.GIF).increment(1);
		}
		else
		{
			context.getCounter(ImageCounter.type.OTHERS).increment(1);
		}
	
		context.write(inputKey, new IntWritable(1));
	}
}
