package localhost4;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntPair;
import tools.IntTriple;
import tools.IntVector;
import tools.IntVector1;

// 向量法需要的矩阵转换器(Hadoop)
// 输入形式：行号 列号 值
// 输出形式：行号 列号 值 列号 值 ...

class TransformMapper extends Mapper<LongWritable, Text, IntPair, IntPair>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntPair(x, y), new IntPair(y, val));
	}
}

class TransformReducer extends Reducer<IntPair, IntPair, IntWritable, IntVector>
implements Constant{
@Override
public void reduce(IntPair key, Iterable<IntPair> values, Context context)
throws IOException, InterruptedException{
	ArrayList<Integer> list = new ArrayList<Integer>();
	Iterator<IntPair> it = values.iterator();
	while(it.hasNext()){
		IntPair ip = it.next();
		list.add(ip.getFirst().get());
		list.add(ip.getSecond().get());
	}
	IntWritable[] array = new IntWritable[list.size()];
	for(int i=0;i<list.size();i++){
		array[i] = new IntWritable(list.get(i));
	}
	context.write(key.getFirst(), new IntVector(new ArrayWritable(IntWritable.class, array)));
	}
}

public class MatrixTransform extends Configured implements Tool, Constant{
	
	public static class FirstPartitioner
	extends Partitioner<IntPair, IntPair>{
	@Override
	public int getPartition(IntPair key, IntPair value, int numPartitions){
		return Math.abs(key.getFirst().hashCode()*127)%numPartitions;
	}
	}
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator() {
			super(IntPair.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			IntPair it1 = (IntPair)w1;
			IntPair it2 = (IntPair)w2;
			return it1.getFirst().compareTo(it2.getFirst());
		}
	}
	
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Transform");
		
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setReducerClass(TransformReducer.class);
		job.setMapperClass(TransformMapper.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		//job.setReducerClass(TestReducer.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntPair.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixTransform(), args);
		System.exit(exitCode);
	}
}
