package MultipleOutputs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputReducer extends
		Reducer<LongWritable, Text, NullWritable, Text> {
	Random rm=new Random();
	int max=3;
	int min=1;
	private MultipleOutputs<NullWritable,Text> multipleOutputs;
	protected void setup(Context context)
	{
		multipleOutputs=new MultipleOutputs<NullWritable,Text>(context);
	}
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		NullWritable ke=NullWritable.get();
		int range = max - min + 1;
		int randomNum =  rm.nextInt(range) + min;
		System.out.println(randomNum);
		
	for(Text value:values)
		switch(randomNum)
		{
		case 1:
			multipleOutputs.write(ke, value,new String("/user/vinodh/multiouts/file1") );
			break;
		case 2:
			multipleOutputs.write(ke, value,new String("/user/vinodh/multiouts/file2") );
			break;
		case 3:
			multipleOutputs.write(ke, value,new String("/user/vinodh/multiouts/file3") );
			break;
		
		}
		
	}

}
