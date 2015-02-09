package IOFormatAndInpustSplits.InputFormats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;




public class MyCombineFileRecordReader extends
		RecordReader<LongWritable, Text> {
	
	LineRecordReader linerecordreader;
	public Configuration conf;
	
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		linerecordreader.close();
	}
	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return linerecordreader.getCurrentKey();
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return linerecordreader.getCurrentValue();
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return linerecordreader.getProgress();
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	FileSplit filesplit=(FileSplit)split;
		this.conf=context.getConfiguration();
		Path path=filesplit.getPath();
		linerecordreader = new LineRecordReader();
		linerecordreader.initialize(filesplit, context);
		
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return linerecordreader.nextKeyValue();
	}
	
}
