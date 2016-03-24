package localhost5;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntPair;
import tools.IntTriple;
import tools.IntVector;

// 外积法需要的矩阵转换器(Hadoop)
// 输入形式：行号 列号 值
// 输出形式：A向量的元素数 A向量 ... B向量
// 两个输出文件 (Based on MatrixTransform2)

class TransformMapperA3 extends Mapper<LongWritable, Text, IntTriple, IntTriple>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntTriple(y, 0, x), new IntTriple(x, val, 0));
	}
}

class TransformMapperB3 extends Mapper<LongWritable, Text, IntTriple, IntTriple>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntTriple(x, 1, y), new IntTriple(y, val, 1));
	}
}

class TransformReducer3 extends Reducer<IntTriple, IntTriple, IntWritable, IntVector> {
	private MultipleOutputs<IntWritable, IntVector> multipleOutputs;
	private int ACOL;
	
	@Override
	protected void cleanup(Reducer<IntTriple, IntTriple, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(Reducer<IntTriple, IntTriple, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<IntWritable, IntVector>(context);
		ACOL = context.getConfiguration().getInt("ACOL", -1);
	}

	@Override
	public void reduce(IntTriple key, Iterable<IntTriple> values, Context context)
			throws IOException, InterruptedException {
		int row = key.getFirst().get();
		LinkedList<Integer> list = new LinkedList<Integer>();
		Iterator<IntTriple> it = values.iterator();
		int n = 0;
		while (it.hasNext()) {
			IntTriple ip = it.next();
			if (ip.getThird().get() == 0) {
				n++;
			}
			list.add(ip.getFirst().get());
			list.add(ip.getSecond().get());
		}
		IntWritable[] array = new IntWritable[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = new IntWritable(list.get(i));
		}
		
		if(row <= ACOL / 2){
			multipleOutputs.write("A", new IntWritable(n), new IntVector(new ArrayWritable(IntWritable.class, array)));
		}else{
			multipleOutputs.write("B", new IntWritable(n), new IntVector(new ArrayWritable(IntWritable.class, array)));
		}
	}
}

public class MatrixTransform3 extends Configured implements Tool, Constant{
	
	public static class FirstPartitioner
	extends Partitioner<IntTriple, IntPair>{
	@Override
	public int getPartition(IntTriple key, IntPair value, int numPartitions){
		return Math.abs(key.getFirst().hashCode()*127)%numPartitions;
	}
	}
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator() {
			super(IntTriple.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			IntTriple it1 = (IntTriple)w1;
			IntTriple it2 = (IntTriple)w2;
			return it1.getFirst().compareTo(it2.getFirst());
		}
	}
	
	public static class KeyComparator extends WritableComparator{
		protected KeyComparator(){
			super(IntTriple.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			IntTriple it1 = (IntTriple)w1;
			IntTriple it2 = (IntTriple)w2;
			return it1.compareTo(it2);
		}
	}
	
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 3){
			System.err.printf("Usage: %s [generic options] <input1>  <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		int acol;
		if((acol = conf.getInt("ACOL", -1)) == -1){
			System.err.println("ACOL is needed\n");
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Transform");
		
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransformMapperA3.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransformMapperB3.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(TransformReducer3.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);
		//job.setReducerClass(TestReducer.class);
		
		MultipleOutputs.addNamedOutput(job, "A", TextOutputFormat.class, IntWritable.class, IntVector.class);
		MultipleOutputs.addNamedOutput(job, "B", TextOutputFormat.class, IntWritable.class, IntVector.class);
		
		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntTriple.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixTransform3(), args);
		System.exit(exitCode);
	}
}
