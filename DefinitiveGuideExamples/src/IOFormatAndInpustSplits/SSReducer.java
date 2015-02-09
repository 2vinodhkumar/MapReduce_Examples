package IOFormatAndInpustSplits;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SSReducer extends Reducer<Employee, IntWritable, Text, IntWritable> {

	public void reduce(Employee emp,Iterable<IntWritable> vals,Context context) throws IOException, InterruptedException
	{
		String empName = emp.getEmpName();

		Iterator<IntWritable> value = vals.iterator();
		while(value.hasNext())
		{
			
		context.write(new Text(empName), value.next());
		//output.collect(empName, values.next());
		}
	}
}
