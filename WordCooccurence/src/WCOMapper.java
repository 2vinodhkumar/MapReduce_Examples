import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WCOMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public  IntWritable one=new IntWritable(1);
	public Text keyOut=new Text();
	public void map(LongWritable key,Text values,Context context) throws InterruptedException, IOException
	{
		String val=values.toString();
		String[] elements=val.split("\\W+");
//here we are not considering if the lines consist of single word only		
		StringTokenizer tokens=new StringTokenizer(val);
		 for (int i=0;i<elements.length;i++)
		 {
			 keyOut.set(elements[i]+":"+elements[i+1]);
			 context.write(keyOut, one);
		 }
	}

}
