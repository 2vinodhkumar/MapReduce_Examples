package IOFormatAndInpustSplits.InputFormats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ExcludeDIRFilter implements PathFilter {

	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
//		path.getFileSystem(getConf());
		return true;
	}

}
