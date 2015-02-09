import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AWLMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	public void map(LongWritable key,Text value,Context context)
	{
		try{

			Text keyOut=new Text();
			IntWritable valOut=new IntWritable();
			String data=value.toString();
			
			String[] splits =data.split("\\W+");
			
			String ch="";
			for(int i=0;i<splits.length;i++)
			{
				
				if(splits[i].length()>0)
				{
				ch=splits[i].substring(0, 1);
				keyOut.set(ch);
				valOut.set(splits[i].length());
				context.write(keyOut, valOut);
				}
			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
