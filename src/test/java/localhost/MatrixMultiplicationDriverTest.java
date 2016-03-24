package localhost;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class MatrixMultiplicationDriverTest {
	public static class OutputLogFilter implements PathFilter {
	    public boolean accept(Path path) {
	      return !path.getName().startsWith("_");
	    }
	  }
	@Test
	public void test() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		//conf.setInt("mapreduce.task.io.sort.mb", 1);
		
		Path input1 = new Path("A.data");
		Path input2 = new Path("B.data");
		Path output = new Path("output");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);		// delete old output
		
		MatrixMultiplication driver = new MatrixMultiplication();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{
			input1.toString(), input2.toString(), output.toString()	
		}) ;
		assertThat(exitCode, is(0));
		checkOutput(conf, output);
	}
	  private void checkOutput(Configuration conf, Path output) throws IOException {
		    FileSystem fs = FileSystem.getLocal(conf);
		    Path[] outputFiles = FileUtil.stat2Paths(
		        fs.listStatus(output, new OutputLogFilter()));
		    assertThat(outputFiles.length, is(1));
		    
		    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
		    BufferedReader expected = asBufferedReader(fs.open(new Path("expected.txt")));
		    String expectedLine;
		    while ((expectedLine = expected.readLine()) != null) {
		      assertThat(actual.readLine(), is(expectedLine));
		    }
		    assertThat(actual.readLine(), nullValue());
		    actual.close();
		    expected.close();
		  }
		  
		  private BufferedReader asBufferedReader(InputStream in) throws IOException {
		    return new BufferedReader(new InputStreamReader(in));
		  }
}
