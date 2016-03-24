package localhost5;

import java.io.IOException;
import java.util.LinkedList;
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
// 两个输出文件
// A的列向量每段长为LA，B的行向量每段长为LB(Based on MatrixTransform4)

class TransformMapperA4 extends Mapper<LongWritable, Text, IntTriple, IntPair>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntTriple(y, 0, x), new IntPair(x, val));
	}
}

class TransformMapperB4 extends Mapper<LongWritable, Text, IntTriple, IntPair>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntTriple(x, 1, y), new IntPair(y, val));
	}
}

class TransformReducer4 extends Reducer<IntTriple, IntPair, IntWritable, IntVector> {
	private MultipleOutputs<IntWritable, IntVector> multipleOutputs;
	private int ACOL;
	private int LA,LB;				// 矩阵A列向量的段长、矩阵B行向量的段长
	
	private LinkedList<LinkedList<Integer>> listA = new LinkedList<LinkedList<Integer>>();
	
	@Override
	protected void cleanup(Reducer<IntTriple, IntPair, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void setup(Reducer<IntTriple, IntPair, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<IntWritable, IntVector>(context);
		ACOL = context.getConfiguration().getInt("ACOL", -1);
		LA = context.getConfiguration().getInt("LA", -1);
		LB = context.getConfiguration().getInt("LB", -1);
	}
	
	private LinkedList<LinkedList<Integer>> breakVector(Iterable<IntPair> values, int Len){
		LinkedList<LinkedList<Integer>> list = new LinkedList<LinkedList<Integer>>();
		LinkedList<Integer> l = new LinkedList<Integer>();
		IntPair t = values.iterator().next();				// 初始化
		//System.out..println(t);
		int cursec = (t.getFirst().get()-1) / Len;			// 段号从0开始计数
		l.add(t.getFirst().get());
		l.add(t.getSecond().get());
		for(IntPair ip : values){
			//System.out..println(ip);
			int sec = (ip.getFirst().get()-1) /  Len;				// 计算段号
			if(sec == cursec){
				l.add(ip.getFirst().get());
				l.add(ip.getSecond().get());
			}else{
				list.add(l);
				l = new LinkedList<Integer>();
				l.add(ip.getFirst().get());
				l.add(ip.getSecond().get());
				cursec = sec;
			}
		}
		list.add(l);
		return list;
	}

	@Override
	public void reduce(IntTriple key, Iterable<IntPair> values, Context context)
			throws IOException, InterruptedException {
		//System.out..println("key=" + key);
		if(key.getSecond().get() == 0){			// 矩阵A的列向量
			listA = breakVector(values, LA);
		}else{														// 矩阵B的行向量
			LinkedList<LinkedList<Integer>> listB = breakVector(values, LB);
			//System.out..println("listA="+listA);
			//System.out..println("listB="+listB);
			int row = key.getFirst().get();
			// 遍历所有的分段，两两组成新向量输出
			for(LinkedList<Integer> la : listA){	
				for(LinkedList<Integer> lb : listB){
					int n = la.size() / 2;				// 向量段的元素数
					IntWritable[] array = new IntWritable[la.size() + lb.size()];
					int i;
					for(i=0;i<la.size();i++){
						array[i] = new IntWritable(la.get(i));
					}
					for(int j=0;j<lb.size();j++){
						array[i+j] = new IntWritable(lb.get(j));
					}
					
					if(row <= ACOL / 2){
						multipleOutputs.write("A", new IntWritable(n), new IntVector(new ArrayWritable(IntWritable.class, array)));
					}else{
						multipleOutputs.write("B", new IntWritable(n), new IntVector(new ArrayWritable(IntWritable.class, array)));
					}
				}
			}
			listA.clear();
		}
		
	}
}

public class MatrixTransform4 extends Configured implements Tool, Constant{
	
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
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			IntTriple it1 = (IntTriple)w1;
			IntTriple it2 = (IntTriple)w2;
			int cmp = it1.getFirst().compareTo(it2.getFirst());
			if(cmp != 0){
				return cmp;
			}else{
				return it1.getSecond().compareTo(it2.getSecond());
			}
		}
	}
	
	public static class KeyComparator extends WritableComparator{
		protected KeyComparator(){
			super(IntTriple.class, true);
		}
		@SuppressWarnings("rawtypes")
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
		if(conf.getInt("ACOL", -1) == -1 ||
				conf.getInt("LA", -1) == -1 ||
				conf.getInt("LB", -1) == -1){
			System.err.println("ACOL、LA and LB is needed\n");
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Transform");
		
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransformMapperA4.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransformMapperB4.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(TransformReducer4.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);
		//job.setReducerClass(TestReducer.class);
		
		MultipleOutputs.addNamedOutput(job, "A", TextOutputFormat.class, IntWritable.class, IntVector.class);
		MultipleOutputs.addNamedOutput(job, "B", TextOutputFormat.class, IntWritable.class, IntVector.class);
		
		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntPair.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixTransform4(), args);
		System.exit(exitCode);
	}
}
