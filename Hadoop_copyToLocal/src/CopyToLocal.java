import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;




public class CopyToLocal {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String sourcePathHdfs=args[0];
		String destinationPath=args[1];
		new CopyToLocal().copyToLocal(sourcePathHdfs, destinationPath);
		
	}
	public void copyToLocal(String source,String destination) throws IOException
	{
		Configuration cfg=new Configuration();
		cfg.addResource(new Path("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		FileSystem fs=FileSystem.get(cfg);
		FSDataInputStream in=fs.open(new Path(source));
		OutputStream out=new BufferedOutputStream(new FileOutputStream(new File(destination)));
		
		byte[] b=new byte[1024];
		
		int num;
		
		while((num=in.read(b))>0)
		{
			out.write(b, 0, num);
		}
		in.close();
		out.close();
	}

}
