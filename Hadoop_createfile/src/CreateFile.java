import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CreateFile {

	private String corePath;
	private String hdfsPath;
	private String filePath;
	
	public CreateFile(String corePath, String hdfsPath, String filePath) {
		super();
		this.corePath = corePath;
		this.hdfsPath = hdfsPath;
		this.filePath = filePath;
	}
	public static void main(String[] args)
	{
		String core=new String("/home/vinodh/hadoop/hadoop-1.2.1/conf/core-site.xml");
		String hdfs=new String("/home/vinodh/hadoop/hadoop-1.2.1/conf/hdfs-site.xml");
		CreateFile cf=new CreateFile(core,hdfs,"app/hadoop/vinodh/Hello.txt");
		cf.create();
		cf.copy(new Path("/app/hadoop/tmp/hadoop-installation-using-centos-.pdf"), new Path("app/hadoop/vinodh/Hello.txt"));
	}
	public void  create()
	{
		Configuration cfg=new Configuration();
		cfg.addResource(new Path(this.corePath));
		cfg.addResource(new Path(this.hdfsPath));
		try{
		FileSystem fileSystem =FileSystem.get(cfg);
		if(fileSystem.exists(new Path(this.filePath)))
		{
			System.out.println("Already Exisits");
		}
		else
		{
			FSDataOutputStream out=fileSystem.create(new Path(this.filePath));
			out.close();
		}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public void copy(Path source,Path destination)
	{
		Configuration cfg=new Configuration();
		cfg.addResource(new Path(this.corePath));
		cfg.addResource(new Path(this.hdfsPath));
		try {
			FileSystem fileSystem=FileSystem.get(cfg);
			FSDataInputStream in=fileSystem.open(source);
			FSDataOutputStream out=fileSystem.create(destination);
			

			    byte[] b = new byte[1024];
			    int numBytes = 0;
			    while ((numBytes = in.read(b)) > 0) {
			        out.write(b, 0, numBytes);
			    }

			    // Close all the file descripters
			    in.close();
			    out.close();
			    fileSystem.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
}
