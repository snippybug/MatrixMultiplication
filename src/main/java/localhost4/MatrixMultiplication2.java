package localhost4;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntTriple;
import tools.IntVector;

// 基于Hadoop的矩阵乘法程序
// 方法五(向量法)——有序版
// 文件输入格式：行号 [列号 值] [列号 值] ...
// 修改Mapper的输出(Based on MatrixMultiplication)

// Mapper输出：<(行号，0, -1)，行向量>
class MatrixAMapper2 extends Mapper<LongWritable, Text, IntTriple, IntVector> {
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		// System.out.println("MapperA start--------------");
		// 矩阵A输出([行号 0 -1]，[向量 -1])
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int row = scan.nextInt(); // 行号
		// 读入向量
		LinkedList<Integer> list = new LinkedList<Integer>();
		while (scan.hasNextInt()) {
			list.add(scan.nextInt());
		}
		scan.close();

		IntWritable[] array = new IntWritable[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = new IntWritable(list.get(i));
		}

		context.write(new IntTriple(row, 0, -1),
				new IntVector(new ArrayWritable(IntWritable.class, array)));
		// System.out.println("MapperA end----------------");
	}
}

// Mapper输出：<(1~ACOL，1，行号)，行向量>
class MatrixBMapper2 extends Mapper<LongWritable, Text, IntTriple, IntVector> {
	private int ACOL;

	@Override
	protected void setup(Mapper<LongWritable, Text, IntTriple, IntVector>.Context context)
			throws IOException, InterruptedException {
		ACOL = context.getConfiguration().getInt("ACOL", -1);
	}

	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		// 矩阵B输出([1~ACOL 1 行号]，[向量])
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int row = scan.nextInt();
		// 读入向量
		LinkedList<Integer> list = new LinkedList<Integer>();
		while (scan.hasNextInt()) {
			list.add(scan.nextInt());
		}
		scan.close();
		IntWritable[] array = new IntWritable[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = new IntWritable(list.get(i));
		}

		for (int i = 1; i <= ACOL; i++) {
			context.write(new IntTriple(i, 1, row),
					new IntVector(new ArrayWritable(IntWritable.class, array)));
		}
		// System.out.println("MapperB end----------------");
	}
}

class MatrixReducer2 extends Reducer<IntTriple, IntVector, IntWritable, IntVector> {
	private int ACOL;
	private IntWritable[] va;
	private int index;				// 当前下标
	private int row;					// 当前行号
	private int[] buf;					// 累加和

	@Override
	protected void setup(Reducer<IntTriple, IntVector, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		ACOL = context.getConfiguration().getInt("ACOL", -1);
		buf = new int[ACOL+1];
	}

	@Override
	public void reduce(IntTriple key, Iterable<IntVector> values, Context context)
			throws IOException, InterruptedException {
		//System.out.println("key="+key);
		if(key.getSecond().get() == 0){				// A的行向量最先到达
			if(index != 0){				// buf内已有数据
				// 生成新的Vector
				LinkedList<Integer> list = new LinkedList<Integer>();
				for (int j = 1; j <= ACOL; j++) {
					if (buf[j] != 0) {
						list.add(j);
						list.add(buf[j]);
					}
				}
				IntWritable[] array = new IntWritable[list.size()];
				for (int j = 0; j < list.size(); j++) {
					array[j] = new IntWritable(list.get(j));
				}
				context.write(new IntWritable(row), new IntVector(new ArrayWritable(IntWritable.class, array)));
			}
			// 重新初始化
			row = key.getFirst().get();
			va = (IntWritable[] )values.iterator().next().getVector().toArray();
			index = 0;
			for(int i=0;i<buf.length;i++){
				buf[i] = 0;
			}
		}else if(row == key.getFirst().get() && index < va.length){															// B的行向量按行号从小到大到达
			IntWritable[] vb = (IntWritable[]) values.iterator().next().getVector().toArray();
			int i = va[index].get();
			int j = key.getThird().get();
			if(i < j){
				do{
					index += 2;
					if(index >= va.length){
						break;
					}
					i = va[index].get();
				}while(i < j);
			}
			if (i == j){
				int p = va[index+1].get();
				for(int k=0;k<vb.length;k+=2){
					buf[vb[k].get()] += p * vb[k+1].get(); 
				}
				index += 2;
			}
		}
	}

	@Override
	protected void cleanup(Reducer<IntTriple, IntVector, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		if(index != 0){
			// 生成新的Vector
			LinkedList<Integer> list = new LinkedList<Integer>();
			for (int j = 1; j <= ACOL; j++) {
				if (buf[j] != 0) {
					list.add(j);
					list.add(buf[j]);
				}
			}
			IntWritable[] array = new IntWritable[list.size()];
			for (int j = 0; j < list.size(); j++) {
				array[j] = new IntWritable(list.get(j));
			}
			context.write(new IntWritable(row), new IntVector(new ArrayWritable(IntWritable.class, array)));
		}
	}
	
}

public class MatrixMultiplication2 extends Configured implements Tool, Constant {

	public static class FirstPartitioner extends Partitioner<IntTriple, IntVector> {
		@Override
		public int getPartition(IntTriple key, IntVector value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
		}
	}

	// @Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		if (conf.getInt("ACOL", -1) == -1) {
			System.err.println("Error: Please tell the number of columns of matrix A by setting variable ACOL");
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Matrix Multiplication");

		job.setJarByClass(getClass());

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixAMapper2.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixBMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(MatrixReducer2.class);
		// job.setReducerClass(TestReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);

		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MatrixMultiplication2(), args);
		System.exit(exitCode);
	}
}
