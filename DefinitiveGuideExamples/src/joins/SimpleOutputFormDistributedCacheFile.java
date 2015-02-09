package joins;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleOutputFormDistributedCacheFile extends Configured implements
		Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration cfg=getConf();
		
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
	
		DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/user/vinodh/joins/dc/emp_Table.csv"), cfg);
		URI[] files=DistributedCache.getCacheFiles(cfg);
		
		FileSystem fs =FileSystem.get(cfg);
		System.out.println(files[0]);
		FSDataInputStream fdin=fs.open(new Path(files[0]));
		
		BufferedReader bf=new BufferedReader(new InputStreamReader(fdin));
		String data;
		while((data=bf.readLine())!=null)
		{
			System.out.println(data);
		}
		return 0;
	}
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SimpleOutputFormDistributedCacheFile(), args);
		
	}

}
