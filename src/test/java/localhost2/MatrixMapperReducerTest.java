package localhost2;

import java.io.IOException;
import java.util.Arrays;

import org.junit.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import tools.*;

public class MatrixMapperReducerTest {
		@Test
		public void testMapperA()
		throws IOException, InterruptedException{
			MapDriver<LongWritable, Text, IntWritable, IntTriple> driver = new MapDriver<LongWritable, Text, IntWritable, IntTriple>()
				.withMapper(new MatrixAMapper());
			
			driver.addInput(new LongWritable(1), new Text("1\t1\t1"));
			driver.addInput(new LongWritable(2), new Text("1\t3\t3"));
			driver.addInput(new LongWritable(3), new Text("2\t1\t4"));
			driver.addInput(new LongWritable(4), new Text("2\t2\t5"));
			
			driver.addOutput(new IntWritable(1), new IntTriple(0, 1,1));
			driver.addOutput(new IntWritable(3), new IntTriple(0, 1, 3));
			driver.addOutput(new IntWritable(1), new IntTriple(0, 2, 4));
			driver.addOutput(new IntWritable(2), new IntTriple(0, 2, 5));
			
	//		driver.runTest();
	}
		@Test
		public void testReducer()
				throws IOException, InterruptedException{
				ReduceDriver<IntWritable, IntTriple, IntPair, IntWritable> driver = new ReduceDriver<IntWritable, IntTriple, IntPair, IntWritable>();
				driver.withReducer(new MatrixReducer());
						
				driver.addInput(new IntWritable(1), Arrays.asList(new IntTriple(0, 1, 1), new IntTriple(0, 2, 4), new IntTriple(1, 1, 10), new IntTriple(1, 2, 15)));
				driver.addOutput(new IntPair(1, 1), new IntWritable(10));
				driver.addOutput(new IntPair(1, 2), new IntWritable(15));
				driver.addOutput(new IntPair(2, 1), new IntWritable(40));
				driver.addOutput(new IntPair(2, 2), new IntWritable(60));
				
//				driver.runTest();
			}
}
