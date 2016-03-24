package localhost;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

import tools.*;

public class MatrixMapperTest {
	@Test
	public void testMapperA()
	throws IOException, InterruptedException{
		MapDriver<LongWritable, Text, IntTriple, IntPair> driver = new MapDriver<LongWritable, Text, IntTriple, IntPair>()
			.withMapper(new MatrixAMapper());
		
		driver.addInput(new LongWritable(1), new Text("1\t1\t1"));
		driver.addInput(new LongWritable(2), new Text("1\t2\t2"));
		driver.addInput(new LongWritable(3), new Text("2\t1\t4"));
		
		driver.addOutput(new IntTriple(1, 1, 0), new IntPair(1,1));
		driver.addOutput(new IntTriple(1, 2, 0), new IntPair(1,1));
		driver.addOutput(new IntTriple(1, 1, 0), new IntPair(2, 2));
		driver.addOutput(new IntTriple(1, 2, 0), new IntPair(2, 2));
		driver.addOutput(new IntTriple(2, 1, 0), new IntPair(1, 4));
		driver.addOutput(new IntTriple(2, 2, 0), new IntPair(1, 4));
		
		
		//driver.runTest()
	}
	@Test
	public void testMapperB()
	throws IOException, InterruptedException{
		MapDriver<LongWritable, Text, IntTriple, IntPair> driver = new MapDriver<LongWritable, Text, IntTriple, IntPair>()
				.withMapper(new MatrixBMapper());
		
		driver.addInput(new LongWritable(1), new Text("1\t1\t10"));
		driver.addInput(new LongWritable(2), new Text("1\t2\t15"));
		driver.addInput(new LongWritable(3), new Text("3\t2\t9"));
		
		driver.addOutput(new IntTriple(1, 1, 1), new IntPair(1, 10));
		driver.addOutput(new IntTriple(2, 1, 1), new IntPair(1, 10));
		driver.addOutput(new IntTriple(3, 1, 1), new IntPair(1, 10));
		driver.addOutput(new IntTriple(4, 1, 1), new IntPair(1, 10));
		
		driver.addOutput(new IntTriple(1, 2, 1), new IntPair(1, 15));
		driver.addOutput(new IntTriple(2, 2, 1), new IntPair(1, 15));
		driver.addOutput(new IntTriple(3, 2, 1), new IntPair(1, 15));
		driver.addOutput(new IntTriple(4, 2, 1), new IntPair(1, 15));
		
		driver.addOutput(new IntTriple(1, 2, 1), new IntPair(3, 9));
		driver.addOutput(new IntTriple(2, 2, 1), new IntPair(3, 9));
		driver.addOutput(new IntTriple(3, 2, 1), new IntPair(3, 9));
		driver.addOutput(new IntTriple(4, 2, 1), new IntPair(3, 9));
		
		//driver.runTest();
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testReducer()
		throws IOException, InterruptedException{
		ReduceDriver<IntTriple, IntPair, IntPair, IntWritable> driver = new ReduceDriver<IntTriple, IntPair, IntPair, IntWritable>();
		driver.withReducer(new MatrixReducer());
		
		//ReduceFeeder<IntPair, Text> feeder = new ReduceFeeder<IntPair, Text>(); 
		//List<KeyValueReuseList<IntPair, Text>> list = feeder.sortAndGroup(Arrays.asList(new Pair<IntPair, Text>(new IntPair(1, 1), new Text("a\t1\t3"))
		//		, new Pair<IntPair, Text>(new IntPair(1, 1), new Text("b\t1\t1")), new Pair<IntPair, Text>(new IntPair(1,1), new Text("a\t2\t3"))));
		
		driver.addInput(new IntTriple(1, 1, 0), Arrays.asList(new IntPair(3, 2), new IntPair(1, 3)));
		driver.addInput(new IntTriple(1, 1, 1), Arrays.asList(new IntPair(1, 1)));
		driver.addInput(new IntTriple(1, 2, 0), Arrays.asList(new IntPair(1, 3)));
		driver.addInput(new IntTriple(2, 1, 1), Arrays.asList(new IntPair(1, 3)));
		driver.addOutput(new IntPair(1, 1), new IntWritable(3));
		
		//driver.runTest();
	}
}
