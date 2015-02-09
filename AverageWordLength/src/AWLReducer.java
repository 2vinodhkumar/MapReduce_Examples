import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AWLReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	IntWritable valOut=new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,Context context)
	{
		Iterator<IntWritable> itr=values.iterator();
		int i=0;
		int sum=0;
		while(itr.hasNext())
		{
			i++;
			int rec=itr.next().get();
			sum=sum+rec;
		}
		valOut.set(sum/i);
		try{
		context.write(key,valOut);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
