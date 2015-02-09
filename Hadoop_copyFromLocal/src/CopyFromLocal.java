import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CopyFromLocal {

	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String localPath=args[0];
		String hdfsFilePath=args[1];
		new CopyFromLocal().copyFromLocal(localPath, hdfsFilePath);
				
	}
	public void copyFromLocal(String localPath,String hdfsFilePath) throws IOException
	{
		Configuration cfg=new Configuration();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(cfg);
		System.out.println(fs.getHomeDirectory());
		InputStream in =new BufferedInputStream(new FileInputStream(new File(localPath)));
		FSDataOutputStream out=fs.create(new Path(hdfsFilePath));
		byte[] bytes=new byte[1024];
		int numOfBytes;
		
		while((numOfBytes=in.read(bytes))>0){
			out.write(bytes, 0, numOfBytes);
		}
		in.close();
		out.close();
		fs.close();
	}

}
