package localhost.matrimultiplication;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


class IntPair implements WritableComparable<IntPair>{
	private IntWritable first;
	private IntWritable second;	
	public IntPair(){
		set(new IntWritable(), new IntWritable());
	}
	public IntPair(IntWritable a, IntWritable b){
		first = a;
		second = b;
	}
	public IntPair(int a, int b){
		set(new IntWritable(a), new IntWritable(b));
	}
	public void set(IntWritable a, IntWritable b){
		first = a;
		second = b;
	}
	public IntWritable getFirst(){
		return first;
	}
	public IntWritable getSecond(){
		return second;
	}
	//@Override
	  public void write(DataOutput out) throws IOException {
	    first.write(out);
	    second.write(out);
	  }

	  //@Override
	  public void readFields(DataInput in) throws IOException {
	    first.readFields(in);
	    second.readFields(in);
	  }
	@Override
	public int hashCode(){
		return first.hashCode() * 163 + second.hashCode();
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof IntPair){
			IntPair ip = (IntPair) o;
			return first.equals(ip.first) && second.equals(ip.second);
		}
		return false;
	}
	@Override
	public String toString(){
		return first.toString() + '\t' + second.toString();
	}
	//@Override
	public int compareTo(IntPair ip){
		int cmp = first.compareTo(ip.first);
		if(cmp != 0){
			return cmp;
		}
		return second.compareTo(ip.second);
	}
}

class IntTriple implements WritableComparable<IntTriple>{
	private IntPair pair;
	private IntWritable key;
	public IntTriple(){
		pair = new IntPair();
		key = new IntWritable();
	}
	public IntTriple(int a, int b, int c){
		pair = new IntPair(a, b);
		key = new IntWritable(c);
	}
	public IntWritable getFirst(){
		return pair.getFirst();
	}
	public IntWritable getSecond(){
		return pair.getSecond();
	}
	public IntWritable getThird(){
		return key;
	}
	public IntPair getPair(){
		return pair;
	}
	public void readFields(DataInput in) throws IOException {
		pair.readFields(in);
		key.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		pair.write(out);
		key.write(out);
	}
	public int compareTo(IntTriple it) {
		int cmp = key.compareTo(it.getThird());
		if(cmp != 0){
			return cmp;
		}
		return pair.compareTo(it.pair);
	}
	
	@Override
	public int hashCode(){
		return pair.hashCode();
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof IntTriple){
			IntTriple ip = (IntTriple) o;
			return pair.equals(ip.getPair()) && key.equals(ip.getThird());
		}
		return false;
	}
	@Override
	public String toString(){
		return pair.toString() + '\t' + key.toString();
	}
}

// SHOULD be repaired
interface Constant{
	int AROW = 4;
	int ACOL = 3;
	int BROW = 3;
	int BCOL = 2;
}

class MatrixAMapper extends Mapper<LongWritable, Text, IntTriple, IntPair>
	implements Constant{
	@Override
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		String line = value.toString();
		String[] strs = line.split("\t");
		assert(strs.length == 3);
		//WARNING: if input data is separated by other delims, parseInt will have an exception
		int[] values= {Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Integer.parseInt(strs[2])};
		// 如果读入的三个数字为i, j, v，则表示A[i][j]=v. 矩阵A的任意点i、j都会影响结果矩阵的第i行所有元素，元素个数为BCOL
		// 将所有受影响的矩阵点加入Reducer中
		for(int i=1;i<=BCOL;i++){
			context.write(new IntTriple(values[0], i, 0), new IntPair(values[1], values[2]));
		}
	}
}

class MatrixBMapper extends Mapper<LongWritable, Text, IntTriple, IntPair>
	implements Constant{
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		String[] strs = line.split("\t");
		assert(strs.length == 3);
		int[] values= {Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Integer.parseInt(strs[2])};
		// 与MatrixAMapper类似。只是矩阵B的任意点i、j影响的是结果矩阵的第j列，元素个数为AROW
		for(int i=1;i<=AROW;i++){
			context.write(new IntTriple(i, values[1], 1), new IntPair(values[0], values[2]));
		}
	}
}

class TestReducer extends Reducer<IntTriple, IntPair, IntPair, IntWritable>{
	private static int num;
	@Override
	public void reduce(IntTriple key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException{
		context.write(new IntPair(-1, -1), new IntWritable(num++));
		Iterator<IntPair> it = values.iterator();
		while(it.hasNext()){
			IntPair temp = it.next();
			context.write(new IntPair(100*key.getFirst().get()+key.getSecond().get(), key.getThird().get()), 
					new IntWritable(100*temp.getFirst().get()+temp.getSecond().get()));
		}
	}
}

class MatrixReducer extends Reducer<IntTriple, IntPair, IntPair, IntWritable>
	implements Constant{
	private static int expected = 0;
	private static int x, y;
	private static int[] vec_save = new int[ACOL];			// 都已初始化为0
	@Override
	public void reduce(IntTriple key, Iterable<IntPair> values, Context context)
		throws IOException, InterruptedException{
		int[] vec2 = new int[ACOL];
		
		// 对于矩阵某点(m, n)，进入Reducer的键值对有四种情况
		// 1. 既有键值为(m, n, 0)，又有(m, n, 1)的数据
		// 2. 只有(m, n, 0)的数据。矩阵某一列为0
		// 3. 只有(m, n, 1)的数据。矩阵某一行为0
		// 4. 既没有(m, n, 0)，也没有(m, n, 1)的数据。这种情况不用考虑
		if(expected == 0){		
			if(expected == key.getThird().get()){		// 情况1和2：保存向量并改变expected
				x = key.getFirst().get();
				y = key.getSecond().get();
				for(int i=0;i<ACOL;i++){
					vec_save[i] = 0;
				}
				Iterator<IntPair> it = values.iterator();
				while(it.hasNext()){
					IntPair temp = it.next();
					vec_save[temp.getFirst().get()-1] = temp.getSecond().get();
				}
				expected = 1;
			}					//else表示情况3，结果为0就不需要再写入了
		}else{				// expected == 1
			if(expected == key.getThird().get()
					&& x == key.getFirst().get() && y == key.getSecond().get()){		// 情况1
				for(int i=0;i<ACOL;i++){
					vec2[i] = 0;
				}
				Iterator<IntPair> it = values.iterator();
				while(it.hasNext()){
					IntPair temp = it.next();
					vec2[temp.getFirst().get()-1] = temp.getSecond().get(); 
				}
				int result = 0;
				for(int i=0;i<ACOL;i++){
					//System.out.println(vec_save[i]+"  "+vec2[i]);
					result += vec_save[i]*vec2[i];
				}
				context.write(key.getPair(), new IntWritable(result));
			}// else表示情况2，结果为0就不需要再写入了
			expected = 0;
		}
	}
}

public class MatrixMultiplication extends Configured implements Tool{

	public static class FirstPartitioner
		extends Partitioner<IntTriple, IntPair>{
		@Override
		public int getPartition(IntTriple key, IntPair value, int numPartitions){
			return Math.abs(key.getPair().hashCode()*127)%numPartitions;
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
	/*
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator() {
			super(IntTriple.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			IntTriple it1 = (IntTriple)w1;
			IntTriple it2 = (IntTriple)w2;
			return it1.getPair().compareTo(it2.getPair());
			
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
		
		job.setReducerClass(MatrixReducer.class);
		//job.setReducerClass(Reducer.class);
		//job.setReducerClass(TestReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntPair.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
