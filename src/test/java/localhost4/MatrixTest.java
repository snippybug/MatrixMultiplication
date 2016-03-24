package localhost4;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import localhost4.MatrixReducer;
import tools.IntTriple;
import tools.IntVector;
import tools.IntVector1;

public class MatrixTest {
	@Test
	public void testReducer()
			throws IOException, InterruptedException{
			ReduceDriver<IntTriple, IntVector1, IntWritable, IntVector> driver = new ReduceDriver<IntTriple, IntVector1, IntWritable, IntVector>();
			driver.withReducer(new MatrixReducer());
					
			IntWritable[] a = {
					new IntWritable(1),
					new IntWritable(1),
					new IntWritable(3),
					new IntWritable(1)
			};
			IntWritable[] b1 = {
					new IntWritable(2),
					new IntWritable(1),
					new IntWritable(3),
					new IntWritable(1)
			};
			IntWritable[] b2 = {
					new IntWritable(1),
					new IntWritable(1),
					new IntWritable(2),
					new IntWritable(1),
					new IntWritable(3),
					new IntWritable(1)
			};
			IntWritable[] b3 = {
					new IntWritable(1),
					new IntWritable(1)
			};
			driver.addInput(new IntTriple(1, 2, 1), Arrays.asList(new IntVector1(new ArrayWritable(IntWritable.class, a), new IntWritable(-1)), 
					new IntVector1(new ArrayWritable(IntWritable.class, b1), new IntWritable(1)),
					new IntVector1(new ArrayWritable(IntWritable.class, b2), new IntWritable(2)),
					new IntVector1(new ArrayWritable(IntWritable.class, b3), new IntWritable(3)))
					);
			IntWritable[] c = {
					new IntWritable(1),
					new IntWritable(1),
					new IntWritable(2),
					new IntWritable(1),
					new IntWritable(3),
					new IntWritable(1)
			};
			driver.addOutput(new IntWritable(1), new IntVector(new ArrayWritable(IntWritable.class, c)));
			driver.runTest();
		}
}
