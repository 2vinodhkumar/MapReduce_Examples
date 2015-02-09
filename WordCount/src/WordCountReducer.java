import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		
		int sum=0;
		for(IntWritable value:values)
		{
			sum+=value.get();
		}
		
//		Thread t=new Thread(r);
//		t.start();
//		t.sleep(1000);
		context.write(key, new IntWritable(sum));
	
	}
		Runnable r=new Runnable(){
			public void run(){
				System.out.println("Thread Called");
				
			}
		};
		
}
