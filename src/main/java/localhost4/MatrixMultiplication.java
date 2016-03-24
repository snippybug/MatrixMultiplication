package localhost4;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntTriple;
import tools.IntVector1;
import tools.IntVector;

// 基于Hadoop的矩阵乘法程序
// 方法五(向量法)——有序版
// 文件输入格式：行号 [列号 值] [列号 值] ...

// Mapper输出：<(行号，0，-1)，(行向量，-1)>
class MatrixAMapper extends Mapper<LongWritable, Text, IntTriple, IntVector1> {
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
				new IntVector1(new ArrayWritable(IntWritable.class, array), new IntWritable(-1)));
		// System.out.println("MapperA end----------------");
	}
}

// Mapper输出：<(1~ACOL，1，行号)，(行向量，行号)>
class MatrixBMapper extends Mapper<LongWritable, Text, IntTriple, IntVector1> {
	private int ACOL;

	@Override
	protected void setup(Mapper<LongWritable, Text, IntTriple, IntVector1>.Context context)
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
					new IntVector1(new ArrayWritable(IntWritable.class, array), new IntWritable(row)));
		}
		// System.out.println("MapperB end----------------");
	}
}

class TestReducer extends Reducer<IntTriple, IntVector1, IntWritable, IntVector1> implements Constant {
	@Override
	public void reduce(IntTriple key, Iterable<IntVector1> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntVector1> it = values.iterator();
		while (it.hasNext()) {
			ArrayWritable array = it.next().getVector();
			context.write(key.getFirst(), new IntVector1(array, null));
		}
		context.write(new IntWritable(999), null);
	}
}

class MatrixReducer extends Reducer<IntTriple, IntVector1, IntWritable, IntVector> {
	private int ACOL;

	@Override
	protected void setup(Reducer<IntTriple, IntVector1, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		ACOL = context.getConfiguration().getInt("ACOL", -1);
	}

	@Override
	public void reduce(IntTriple key, Iterable<IntVector1> values, Context context)
			throws IOException, InterruptedException {
		// values有一个来自A的行向量和若干个来自B的行向量. first代表A(0)或B(1), second代表行号
		Iterator<IntVector1> it = values.iterator();
		IntWritable[] a = (IntWritable[]) it.next().getVector().toArray(); // 第一个是A的行向量
		int[] buf = new int[ACOL + 1]; // 临时存储区(可能有点大!!)
		int index = 0;
		while (it.hasNext()) {
			IntVector1 vec = it.next();
			IntWritable[] b = (IntWritable[]) vec.getVector().toArray();
			int col = a[index].get(); // A中元素的列号
			int row = vec.getValue().get(); // B中向量的行号(行号已按升序排列)
			while (col < row && index + 2 < a.length) {
				index += 2;
				col = a[index].get();
			}
			if (col == row) {
				for (int j = 0; j < b.length; j += 2) {
					buf[b[j].get()] += a[index + 1].get() * b[j + 1].get();
				}
			}
			// 如果col > row，则直接读入下一个B的行向量
		}
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
		context.write(key.getFirst(), new IntVector(new ArrayWritable(IntWritable.class, array)));
	}
}

public class MatrixMultiplication extends Configured implements Tool, Constant {

	public static class FirstPartitioner extends Partitioner<IntTriple, IntVector1> {
		@Override
		public int getPartition(IntTriple key, IntVector1 value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntTriple.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntTriple it1 = (IntTriple) w1;
			IntTriple it2 = (IntTriple) w2;

			return it1.compareTo(it2);
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(IntTriple.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntTriple it1 = (IntTriple) w1;
			IntTriple it2 = (IntTriple) w2;
			return it1.getFirst().compareTo(it2.getFirst());
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
		int col;
		if ((col = conf.getInt("ACOL", -1)) == -1) {
			System.err.println("Error: Please tell the number of columns of matrix A by setting variable ACOL");
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Matrix Multiplication");

		job.setJarByClass(getClass());

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixAMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixBMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(MatrixReducer.class);
		// job.setReducerClass(TestReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setMapOutputKeyClass(IntTriple.class);
		job.setMapOutputValueClass(IntVector1.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
