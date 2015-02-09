import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;

public class Test {

	@org.junit.Test
	public void test() throws Exception
	{
		Configuration cfg=new Configuration();
		cfg.set("fs.default.name", "file:///");
		cfg.set("mapred.job.tracker", "local");
//		cfg.set("mapreduce.framework.name", "local");
		
		Path input=new Path("/home/vinodh/Desktop/test.txt");
		Path output=new Path("/home/vinodh/Desktop/output");
		FileSystem fs=FileSystem.get(cfg);
		fs.delete(output,true);
		Driver dr=new Driver();
		dr.setConf(cfg);
		int ex=dr.run(new String[] {input.toString(),output.toString()});
		
	}
}
