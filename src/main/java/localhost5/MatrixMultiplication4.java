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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntVector;

// 基于Hadoop的矩阵乘法程序
// 外积法
// 输入格式：[A的列向量的元素个数] [A的列向量] [B的行向量]
// 两个文件输入
// mapper的输出改为<行号，行向量> (Based on MatrixMultiplication2)

class MatrixMapper4 extends Mapper<LongWritable, Text, IntWritable, IntVector> {
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		//System.out.println("Map Start--------------");
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int num = scan.nextInt() * 2; // A的列向量元素个数
		// 读入A的列向量
		LinkedList<Integer> listA = new LinkedList<Integer>();
		//System.out.println("Mapper: num="+num);
		for (int i = 0; i < num; i++) {
			listA.add(scan.nextInt());
		}
		//System.out.println("Map: After reading vector A --------------");
		// 剩下的是B的行向量
		LinkedList<Integer> listB = new LinkedList<Integer>();
		while (scan.hasNextInt()) {
			listB.add(scan.nextInt());
		}
		scan.close();

		//System.out.println("Map: After reading vector B--------------");
		for (int i = 0; i < num; i += 2) {
			int x = listA.get(i); // 行号
			int p = listA.get(i + 1); // 乘法系数
			for (int j = 1; j < listB.size(); j += 2) {
				listB.set(j, listB.get(j) * p);
			}
			IntWritable[] array = new IntWritable[listB.size()];
			for (int j = 0; j < array.length; j++) {
				array[j] = new IntWritable(listB.get(j));
			}
			context.write(new IntWritable(x), new IntVector(new ArrayWritable(IntWritable.class, array)));
		}
		/*
		 * int num = key.get(); IntWritable[] array =
		 * (IntWritable[])values.getVector().toArray(); for(int
		 * i=0;i<num*2;i+=2){ int x = array[i].get(); for(int
		 * j=num*2;j<array.length;j+=2){ int y = array[j].get();
		 * context.write(new IntPair(x, y), new
		 * IntWritable(array[x+1].get()*array[y+1].get())); } }
		 */
		 //System.out.println("Map End--------------");
	}
}

class MatrixReducer4 extends Reducer<IntWritable, IntVector, IntWritable, IntVector> {
	private int ACOL;

	@Override
	protected void setup(Reducer<IntWritable, IntVector, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		ACOL = context.getConfiguration().getInt("ACOL", -1);
	}

	@Override
	public void reduce(IntWritable key, Iterable<IntVector> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntVector> it = values.iterator();
		// System.out.println("max="+max);
		int[] buf = new int[ACOL + 1];
		// 第二遍遍历开始计算
		// System.out.print("buf=");
		/*
		for (int i = 0; i < buf.length; i++) {
			System.out.print(" " + buf[i]);
		}
		*/
		while (it.hasNext()) {
			IntVector iv = it.next();
			// System.out.println("vec="+iv);
			IntWritable[] array = (IntWritable[]) iv.getVector().toArray();
			for (int i = 0; i < array.length; i += 2) {
				buf[array[i].get()] += array[i + 1].get();
			}
			// System.out.print("buf=");
			// for(int i=0;i<buf.length;i++){
			// System.out.print(" "+buf[i]);
			// }
		}
		// 生成新的vector
		LinkedList<Integer> list = new LinkedList<Integer>();
		for (int i = 1; i < buf.length; i++) {
			if (buf[i] != 0) {
				list.add(i);
				list.add(buf[i]);
			}
		}
		// System.out.println("Vector="+list);
		IntWritable[] array = new IntWritable[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = new IntWritable(list.get(i));
		}
		context.write(key, new IntVector(new ArrayWritable(IntWritable.class, array)));
		// System.out.println("Reduce End--------------");
	}
}

public class MatrixMultiplication4 extends Configured implements Tool, Constant {

	// @Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input1>  <input2> <output>\n", getClass().getSimpleName());
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

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixMapper4.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixMapper4.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(MatrixReducer4.class);
		job.setMapperClass(MatrixMapper4.class);
		job.setCombinerClass(MatrixReducer4.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MatrixMultiplication4(), args);
		System.exit(exitCode);
	}
}
