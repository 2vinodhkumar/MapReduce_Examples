package joins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EmployeeMapper extends Mapper<LongWritable,Text, Text, Text>{
	
	private URI[] files;
	private HashMap<String,String> locationTable = new HashMap<String,String>();

	
	@Override
	public void setup(Context context) throws IOException
	{
		/**
		 * These Comment are to swith from Generic Command line execution by Jar
		 * i.e adding Files from command line while submitting job through jar file
		 */
//		files = DistributedCache.getCacheFiles(context.getConfiguration());
		
		System.out.println("files:"+ files);
//		Path path = new Path(files[0]);
		Path[] commandLineFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		File file =new File(commandLineFiles[0].getName());
		FileSystem fs = FileSystem.get(context.getConfiguration());
//		FSDataInputStream in = fs.open(path);
//		BufferedReader br  = new BufferedReader(new InputStreamReader(in));
		BufferedReader br  = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        
        String line="";
        while((line = br.readLine())!=null)
        {
                String splits[] = line.split(",");
                //splits[0] is the location code and splits[1] is the location name
                locationTable.put(splits[0], splits[1]);

        }
        
		br.close();
//		in.close();
		
		
	}
	@Override
	public void map(LongWritable key, Text val,Context context) throws IOException,InterruptedException
	{
		
		String[] splits = val.toString().split(",");
		
		if(locationTable.containsKey(splits[6]))
		{
			context.write(new Text(splits[0]+":"+splits[6]), new Text(locationTable.get(splits[6])));
		}
		else
		{
			context.write(new Text(splits[0]+":"+splits[6]),new Text("No Location Code"));
		}
		
		
	}

}


