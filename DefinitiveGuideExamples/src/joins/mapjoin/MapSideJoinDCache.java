package joins.mapjoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinDCache extends Mapper<LongWritable, Text, Text, Text> {
	
	public HashMap<String,String> dept=new HashMap<String,String>();
	public void setup(Context context) throws IOException
	{
		Path[] dFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		File file=new File(dFiles[0].getName());
		BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String deptData;
		while((deptData=br.readLine())!=null)
		{
			String deptsplits[]=deptData.split("\\t");
			dept.put(deptsplits[0], deptsplits[1]);
		}
	}
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String data=value.toString();
		String[] vals=data.split("\\t");
		int len=vals.length;
		//vals[2] is deptid in emp data,0-name,1-address,2-deptid
		if(dept.containsKey(vals[len-1]))
		{
			String deptName=dept.get(vals[len-1]);
			context.write(value, new Text(deptName));
		}
	}

}
