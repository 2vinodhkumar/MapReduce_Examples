package IOFormatAndInputSplits.SplitPreventing.Sorting;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileDATAOutput extends Mapper<NullWritable, BytesWritable, Text, Text> {

	
	
	public void map(NullWritable key,BytesWritable value,Context context) throws IOException, InterruptedException
	{
		byte[] content=value.getBytes();
		String sr=new String(content);
		Text t=new Text(sr);
		FileSplit fs=(FileSplit) context.getInputSplit();
		Path path=fs.getPath();
//		FileSystem filesystem =path.getFileSystem(context.getConfiguration());
		Text fn=new Text(path.toString());
		context.write(fn, t);
	}
	public void run(Context context) throws IOException, InterruptedException
	{
		setup(context);
		while(context.nextKeyValue())
		{
			System.out.println("true");
			map(context.getCurrentKey(),context.getCurrentValue(),context);
		}
		cleanup(context);
		
	}
}
