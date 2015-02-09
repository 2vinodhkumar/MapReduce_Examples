package configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TestConfiguration {

	
	public static void main(String[] args)
	{
		Configuration cfg=new Configuration();
		
		cfg.addResource(new Path("/home/vinodh/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/conf/core-site2.xml"));
		
		System.out.println(cfg.get("color"));
		System.out.println(cfg.get("size"));
		System.out.println(cfg.get("weight"));
		System.out.println(cfg.get("size-weight"));
		System.setProperty("size", "22");
		System.out.println(cfg.get("size-weight"));
		System.out.println(cfg.get("size"));
	}
}
