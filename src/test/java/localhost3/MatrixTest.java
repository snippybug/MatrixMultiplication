package localhost3;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import localhost3.MatrixReducer;
import tools.IntVector2;

public class MatrixTest {
	@Test
	public void testMapperA()
	throws IOException, InterruptedException{
		MapDriver<LongWritable, Text, IntWritable, IntVector2> driver = new MapDriver<LongWritable, Text, IntWritable, IntVector2>()
			.withMapper(new MatrixBMapper());
		
		driver.addInput(new LongWritable(1), new Text("1 1 1 3 1"));
		//driver.addInput(new LongWritable(2), new Text("2 3 1"));
		//driver.addInput(new LongWritable(3), new Text("3 1 1 3 1"));
		
		IntWritable[] a1 = new IntWritable[4];
		a1[0] = new IntWritable(1);
		a1[1] = new IntWritable(1);
		a1[2] = new IntWritable(3);
		a1[3] = new IntWritable(1);
		driver.addOutput(new IntWritable(1), new IntVector2(new ArrayWritable(IntWritable.class, a1), new IntWritable(0), new IntWritable(1)));
		
		//driver.runTest();
	}
	
	@Test
	public void testReducer()
			throws IOException, InterruptedException{
			ReduceDriver<IntWritable, IntVector2, IntWritable, IntVector2> driver = new ReduceDriver<IntWritable, IntVector2, IntWritable, IntVector2>();
			driver.withReducer(new MatrixReducer());
					
			IntWritable[] a1 = new IntWritable[4];
			a1[0] = new IntWritable(1);
			a1[1] = new IntWritable(1);
			a1[2] = new IntWritable(3);
			a1[3] = new IntWritable(1);
			driver.addInput(new IntWritable(1), Arrays.asList(new IntVector2(new ArrayWritable(IntWritable.class, a1), new IntWritable(0), new IntWritable(1)), new IntVector2(new ArrayWritable(IntWritable.class, a1), new IntWritable(0), new IntWritable(1))
					));
			
			driver.runTest();
		}
}
