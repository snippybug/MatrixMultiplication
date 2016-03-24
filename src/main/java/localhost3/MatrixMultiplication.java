package localhost3;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntVector2;

// 基于Hadoop的矩阵乘法程序
// 方法五(向量法)——无序版
// 文件输入格式：行号 [列号 值] [列号 值] ...

class MatrixAMapper extends Mapper<LongWritable, Text, IntWritable, IntVector2>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		// 矩阵A输出(行号，[列号 向量 0])
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int row = scan.nextInt();			// 行号
		LinkedList<Integer> list = new LinkedList<Integer>();
		while(scan.hasNextInt()){
			list.add(scan.nextInt());
		}
		scan.close();
		IntWritable[] array = new IntWritable[list.size()];
		for(int i=0;i<list.size();i++){
			array[i] = new IntWritable(list.get(i));
		}
		IntVector2 vec = new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(0), new IntWritable(row));
		context.write(new IntWritable(row), vec);
	}
}

class MatrixBMapper extends Mapper<LongWritable, Text, IntWritable, IntVector2>
	implements Constant{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		// 矩阵B输出(1~ACOL，[列号 向量])
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int row = scan.nextInt();
		// 剩下的存入数组
		LinkedList<Integer> list = new LinkedList<Integer>();
		while(scan.hasNextInt()){
			list.add(scan.nextInt());
		}
		scan.close();
		IntWritable[] array = new IntWritable[list.size()];
		for(int i=0;i<list.size();i++){
			array[i] = new IntWritable(list.get(i));
		}
		IntVector2 vec = new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(1), new IntWritable(row));
		
		for(int i=1;i<=ACOL;i++){
			context.write(new IntWritable(i), vec);
		}
	}
}

// 有序版
class MatrixReducer extends Reducer<IntWritable, IntVector2, IntWritable, IntVector2>
	implements Constant{
@Override
public void reduce(IntWritable key, Iterable<IntVector2> values, Context context)
	throws IOException, InterruptedException{
		// values有一个来自A的行向量和若干个来自B的行向量. first代表A(0)或B(1), second代表行号
		Iterator<IntVector2> it = values.iterator();
		IntWritable[] a = (IntWritable[]) it.next().getVector().toArray();
		int[] buf = new int[ACOL + 1];			// 临时存储区(可能有点大!!)
		int i=0;
		IntWritable[] b = null;
		IntVector2 vec = new IntVector2();		// 冗余
		while(it.hasNext()){
			int col = a[i].get();				// A中元素的列号
			int row = vec.getSecond().get();			// B中向量的行号(行号已按升序排列)
			if(col < row){
				i += 2;
				continue;
			}else if(col == row){
				for(int j=0;j<b.length;j+=2){
					buf[b[j].get()] += a[i+1].get() * b[j+1].get();
				}
			}
			// 若col > row，则读入下一个行向量
			vec = it.next();
			b = (IntWritable[]) vec.getVector().toArray();
		}
		// 生成新的Vector
		LinkedList<Integer> list = new LinkedList<Integer>();
		for(int j=1;j<=ACOL;j++){
			if(buf[j] != 0){
				list.add(j);
				list.add(buf[j]);
			}
		}
		IntWritable[] array = new IntWritable[list.size()];
		for(int j=0;j<list.size();j++){
			array[i] = new IntWritable(list.get(i));
		}
		context.write(key, new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(0), new IntWritable(0)));
	}
}

class TestReducer extends Reducer<IntWritable, IntVector2, IntWritable, IntVector2>
implements Constant{
@Override
public void reduce(IntWritable key, Iterable<IntVector2> values, Context context)
throws IOException, InterruptedException{
		Iterator<IntVector2> it = null;
		for(int i=0;i<4;i++){
			it = values.iterator();
			while(it.hasNext()){
				IntVector2 b = it.next();
				IntWritable[] array = (IntWritable[])b.getVector().toArray();
				context.write(key, new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(0), new IntWritable(0)));
			}
		}
	}
}

// 无序版
class MatrixReducerNO extends Reducer<IntWritable, IntVector2, IntWritable, IntVector2>
implements Constant{
@Override
public void reduce(IntWritable key, Iterable<IntVector2> values, Context context)
throws IOException, InterruptedException{
	// values中有一个来自A的行向量和若干个来自B的行向量. first代表A(0)或B(1), second代表行号
	Iterator<IntVector2> it = values.iterator();

	// 先找到来自A的行向量
	IntWritable[] a = null;
	while(it.hasNext()){
		IntVector2 iv = it.next();
		if(iv.getFirst().get() == 0){
			iv.setSecond(-1);			// 触发数组越界异常
			a = (IntWritable[]) iv.getVector().toArray();
			break;
		}
	}
	int[] buf = new int[ACOL + 1];			// 临时存储区(可能有点大!!)
	int[] va = new int[ACOL + 1];
	for(int j=0;j<a.length;j+=2){			// 转化为标准向量
		va[a[j].get()] = a[j+1].get();
	}
	
	it = values.iterator();
	context.write(new IntWritable(200), null);
	
	while(it.hasNext()){
		IntVector2 b = it.next();
		int row = b.getSecond().get();
		IntWritable[] array = (IntWritable[])b.getVector().toArray();
		context.write(key, new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(0), new IntWritable(0)));
		int p = 0;
			p = va[row];		// 系数
		for(int j=0;j<array.length;j+=2){
			buf[array[j].get()] += p * array[j+1].get();
		}
	}
	// 生成新的Vector
	LinkedList<Integer> list = new LinkedList<Integer>();
	for(int j=1;j<=ACOL;j++){
		if(buf[j] != 0){
			list.add(j);
			list.add(buf[j]);
		}
	}
	IntWritable[] array = new IntWritable[list.size()];
	for(int j=0;j<list.size();j++){
		array[j] = new IntWritable(list.get(j));
	}
	//context.write(key, new IntVector2(new ArrayWritable(IntWritable.class, array), new IntWritable(0), new IntWritable(0)));
}
}

public class MatrixMultiplication extends Configured implements Tool, Constant{
/*
	public static class FirstPartitioner
		extends Partitioner<IntTriple, IntPair>{
		@Override
		public int getPartition(IntTriple key, IntPair value, int numPartitions){
			return Math.abs(key.getPair().hashCode()*127)%numPartitions;
		}
	}
	*/
	/*
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
	*/
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 3){
			System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Multiplication");
		
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixAMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixBMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
//		job.setReducerClass(MatrixReducerNO.class);
		//job.setReducerClass(Reducer.class);
		job.setReducerClass(TestReducer.class);
//		job.setPartitionerClass(FirstPartitioner.class);
		//job.setSortComparatorClass(KeyComparator.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntVector2.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector2.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
