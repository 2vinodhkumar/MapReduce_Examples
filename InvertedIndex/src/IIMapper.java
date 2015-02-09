import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IIMapper extends Mapper<LongWritable, Text, Text, Text> {

	public Text keyWord=new Text();
	public Text file=new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		Path fileName=((FileSplit)context.getInputSplit()).getPath();
        
		String data=value.toString();
		String[] splits=data.trim().split(" ");
		file.set(fileName.getName());

		for(int i=0;i<splits.length;i++)
		{
		
			
			keyWord.set(splits[i]);
			context.write(keyWord, file);
		}
		
		
	}
}
