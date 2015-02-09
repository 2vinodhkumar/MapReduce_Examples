import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WCOReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public IntWritable totalSum=new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum=0;
		Iterator<IntWritable> itr=values.iterator();
		while(itr.hasNext())
		{
			int i=itr.next().get();
			sum=sum+i;
		}
		totalSum.set(sum);
		context.write(key, totalSum);
		
	}

}
