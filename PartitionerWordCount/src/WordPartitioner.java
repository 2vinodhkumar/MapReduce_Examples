import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable val, int reduceTask) {
		// TODO Auto-generated method stub
		int reduce =0;
		
		String keyPart=key.toString();
		if(keyPart.startsWith("v"))
		{
			System.out.println(3%reduceTask);
			return 1;
			
		}
		return 0;
	}

	
}
