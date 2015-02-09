package IOFormatAndInpustSplits;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SSMapper extends Mapper<LongWritable, Text, Employee, IntWritable> {

	Employee emp=new Employee();
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String[] vals=value.toString().split(",");
		System.out.println("name"+vals.length);
		if(vals.length==2)
		{
			String name=vals[0].trim();
			
			Integer id=Integer.parseInt(vals[1].trim());
			emp.setEmpID(id);
			emp.setEmpName(name);
			context.write(emp, new IntWritable(id));
		}
	}
}
