package IOFormatAndInputSplits.SplitPreventing.Sorting;

import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortWholeFile extends Mapper<NullWritable, BytesWritable, NullWritable, Text> {

	public void map(NullWritable key,BytesWritable value )
	{
		String data =new String(value.getBytes());
		StringTokenizer str=new StringTokenizer(data);
		while(str.hasMoreTokens())
		{
			String tmp=str.nextToken();
			
		}
	}
}
