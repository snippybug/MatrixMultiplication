package localhost5;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Scanner;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntPair;
import tools.IntTriple;
import tools.IntVector;
import tools.IntVector1;

// 外积法需要的矩阵转换器(Hadoop)
// 输入形式：行号 列号 值
// 输出形式：A向量的元素数 A向量 ... B向量

class TransformMapperA extends Mapper<LongWritable, Text, IntTriple, IntTriple>{
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

class TransformMapperB extends Mapper<LongWritable, Text, IntTriple, IntTriple>{
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

class TransformReducer extends Reducer<IntTriple, IntTriple, IntWritable, IntVector>
implements Constant{
@Override
public void reduce(IntTriple key, Iterable<IntTriple> values, Context context)
throws IOException, InterruptedException{
	LinkedList<Integer> list = new LinkedList<Integer>();
	Iterator<IntTriple> it = values.iterator();
	int n = 0;
	while(it.hasNext()){
		IntTriple ip = it.next();
		if(ip.getThird().get() == 0){
			n++;
		}
		list.add(ip.getFirst().get());
		list.add(ip.getSecond().get());
	}
	IntWritable[] array = new IntWritable[list.size()];
	for(int i=0;i<list.size();i++){
		array[i] = new IntWritable(list.get(i));
	}
	context.write(new IntWritable(n), new IntVector(new ArrayWritable(IntWritable.class, array)));
	}
}

public class MatrixTransform extends Configured implements Tool, Constant{
	
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
			
			int cmp = it1.getPair().compareTo(it2.getPair());
			if(cmp != 0){
				return cmp;
			}else{
				return it1.getThird().compareTo(it2.getThird());
			}
		}
	}
	
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 3){
			System.err.printf("Usage: %s [generic options] <input1>  <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Transform");
		
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransformMapperA.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransformMapperB.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(TransformReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);
		//job.setReducerClass(TestReducer.class);
		
		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntTriple.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixTransform(), args);
		System.exit(exitCode);
	}
}
