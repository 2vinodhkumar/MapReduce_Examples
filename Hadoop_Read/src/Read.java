import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Read {

	public static void main(String[] args) throws IOException
	{
		Configuration cfg=new Configuration();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(cfg);
		 Path path = new Path("/outputpath/part-r-00000");

		BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(path)));
		String line=br.readLine();
		
		while(line!=null)
		{
			System.out.println(line);
			line=br.readLine();
		}
	}
}
