package localhost6;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import tools.IntPair;
import tools.IntQuad;
import tools.IntTriple;

// 基于Hadoop的矩阵乘法程序
// 分块法
// 输入格式：行号 列号 值
// 方法参考网址：http://www.norstad.org/matrix-multiply/

// MatrixA的大小为I*K，MatrixB的大小为K*J，C=A*B
// 分块后，NIB表示A和C的行数；NKB表示A的列数和B的行数；NJB表示B和C的列数
// IB表示A和C中块的行数，KB表示A中块的列数和B中块的行数，IB表示B和C中块的列数
// 矩阵的行列号从1开始计数，即1 <= i <= N，1 <= k <= K，1 <= j <= J
// 块内行列号从0计数
// 块号从0开始计数，即0 <= ib < NIB，0 <= kb < NKB，0 <= jb < NJB

class MatrixAMapper extends Mapper<LongWritable, Text, IntQuad, IntTriple> {
	private int NJB, IB, KB;

	@Override
	protected void setup(Mapper<LongWritable, Text, IntQuad, IntTriple>.Context context)
			throws IOException, InterruptedException {
		NJB = context.getConfiguration().getInt("NJB", -1);
		IB = context.getConfiguration().getInt("IB", -1);
		KB = context.getConfiguration().getInt("KB", -1);
	}

	// Map的输出为：<(ib, jb, kb, 0)，(i, k, val)>
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		//System.out.println("Map Start--------------");
		Scanner scaner = new Scanner(text.toString());
		int x = scaner.nextInt();
		int y = scaner.nextInt();
		int val = scaner.nextInt();
		scaner.close();
		x--;
		y--;
		for (int jb = 0; jb < NJB; jb++) {
			int ib = x / IB; // 行块号
			int kb = y / KB; // 列块号
			int i = x % IB; // 块内行号
			int k = y % KB; // 块内列号
			context.write(new IntQuad(ib, jb, kb, 0), new IntTriple(i, k, val));
		}
		//System.out.println("Map End--------------");
	}
}

class MatrixBMapper extends Mapper<LongWritable, Text, IntQuad, IntTriple> {
	private int NIB, JB, KB;

	@Override
	protected void setup(Mapper<LongWritable, Text, IntQuad, IntTriple>.Context context)
			throws IOException, InterruptedException {
		NIB = context.getConfiguration().getInt("NIB", -1);
		JB = context.getConfiguration().getInt("JB", -1);
		KB = context.getConfiguration().getInt("KB", -1);
	}

	// Map的输出为：<(ib, jb, kb, 1)，(k, j, val)>
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		//System.out.println("Map Start--------------");
		Scanner scaner = new Scanner(text.toString());
		int x = scaner.nextInt();
		int y = scaner.nextInt();
		int val = scaner.nextInt();
		x--;
		y--;
		for (int ib = 0; ib < NIB; ib++) {
			int kb = x / KB; // 块行号
			int jb = y / JB; // 块列号
			int k = x % KB; // 块内行号
			int j = y % JB; // 块内列号
			context.write(new IntQuad(ib, jb, kb, 1), new IntTriple(k, j, val));
		}
		scaner.close();
		//System.out.println("Map End--------------");
	}
}

class MatrixReducer extends Reducer<IntQuad, IntTriple, IntPair, IntWritable> {
	private static int[][] A, C; // 用于小矩阵乘法运算
	private static int sib = -1, sjb = -1, skb = -1; // 当前计算的矩阵块号
	private int IB, KB, JB;

	@Override
	protected void setup(Reducer<IntQuad, IntTriple, IntPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IB = context.getConfiguration().getInt("IB", -1);
		KB = context.getConfiguration().getInt("KB", -1);
		JB = context.getConfiguration().getInt("JB", -1);
		A = new int[IB][KB];
		C = new int[IB][JB];
	}

	private void clearMatrix(int[][] M, int Row, int Col) {
		for (int i = 0; i < Row; i++) {
			for (int j = 0; j < Col; j++) {
				M[i][j] = 0;
			}
		}
	}

	@Override
	protected void cleanup(Reducer<IntQuad, IntTriple, IntPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if (sib != -1) {
			// 输出最后一个小矩阵
			int ibase = sib * IB;
			int jbase = sjb * JB;
			for (int i = 0; i < IB; i++) {
				for (int j = 0; j < JB; j++) {
					int v = C[i][j];
					if (v != 0) {
						context.write(new IntPair(ibase + i + 1, jbase + j + 1), new IntWritable(v));
					}
				}
			}
		}
	}

	@Override
	public void reduce(IntQuad key, Iterable<IntTriple> values, Context context)
			throws IOException, InterruptedException {
		int ib = key.getA().get();
		int jb = key.getB().get();
		int kb = key.getC().get();
		int m = key.getD().get(); // m=0表示来自矩阵A，m=1来自矩阵B
		if (ib != sib || jb != sjb) {
			// 开始新的(ib,jb)序列
			if (sib != -1) { // 写出上一趟的矩阵C
				int ibase = sib * IB;
				int jbase = sjb * JB;
				for (int i = 0; i < IB; i++) {
					for (int j = 0; j < JB; j++) {
						int v = C[i][j];
						if (v != 0) {
							context.write(new IntPair(ibase + i + 1, jbase + j + 1), new IntWritable(v));
						}
					}
				}
			}
			sib = ib;
			sjb = jb;
			skb = -1;
			clearMatrix(C, IB, JB);
		}
		if (m == 0) {
			skb = kb;
			clearMatrix(A, IB, KB);
			for (IntTriple val : values) {
				int x = val.getFirst().get();
				int y = val.getSecond().get();
				int v = val.getThird().get();
				A[x][y] = v;
			}
		} else if (m == 1) {
			if (kb == skb) {
				for (IntTriple val : values) {
					int k = val.getFirst().get();
					int j = val.getSecond().get();
					int v = val.getThird().get();
					for (int i = 0; i < IB; i++) {
						C[i][j] += v * A[i][k];
					}
				}
			}
			// else 等待返回，因为此时只有A[ib,kb]而没有B[kb,jb]，可得C[ib,jb]=0
		}
	}
}

public class MatrixMultiplication extends Configured implements Tool {
	private static int JB;

	public static class MatrixPartitioner extends Partitioner<IntQuad, IntTriple> {
		@Override
		public int getPartition(IntQuad key, IntTriple value, int numPartitions) {
			return (key.getA().get() * JB + key.getB().get()) % numPartitions;
		}
	}

	// @Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input1>  <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		if ((conf.getInt("IB", -1) == -1) || ((JB = conf.getInt("JB", -1)) == -1) || (conf.getInt("KB", -1) == -1)
				|| (conf.getInt("NIB", -1) == -1) || (conf.getInt("NJB", -1) == -1) || (conf.getInt("NKB", -1) == -1)) {
			System.err.println("Error: Please set the variable IB、JB、KB、NIB、NJB、NKB");
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Matrix Multiplication");

		job.setJarByClass(getClass());

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixAMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixBMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(MatrixReducer.class);
		job.setPartitionerClass(MatrixPartitioner.class);

		job.setMapOutputKeyClass(IntQuad.class);
		job.setMapOutputValueClass(IntTriple.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
