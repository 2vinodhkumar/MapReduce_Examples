import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IIReducer extends Reducer<Text, Text, Text, Text> {

	Text keyOut =new Text();
	Text valOut=new Text();
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		boolean isFirst=true;
		StringBuilder out =new StringBuilder();
		Iterator<Text> itr=values.iterator();
		while(itr.hasNext())
		{
			if(!isFirst)
				out.append(",");
			isFirst=false;
			out.append(itr.next().toString());			
		}
		valOut.set(out.toString());
		context.write(key,valOut );
	}
}
