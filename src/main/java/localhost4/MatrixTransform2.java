package localhost4;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Scanner;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntPair;
import tools.IntVector;

// 向量法需要的矩阵转换器(Hadoop)
// 输入形式：行号 列号 值
// 输出形式：行号 列号 值 列号 值 ...
// 改变了Mapper的输出(Based on MatrixTransform)

class TransformMapper2 extends Mapper<LongWritable, Text, IntPair, IntWritable> {
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int y = scan.nextInt();
		int val = scan.nextInt();
		scan.close();
		context.write(new IntPair(x, y), new IntWritable(val));
	}
}

class TransformReducer2 extends Reducer<IntPair, IntWritable, IntWritable, IntVector> {
	private int currow = -1;
	private LinkedList<Integer> list = new LinkedList<Integer>();

	@Override
	public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int row = key.getFirst().get();
		if (row != currow) {
			if (list.size() != 0) {
				// 写出
				IntWritable[] array = new IntWritable[list.size()];
				for (int i = 0; i < list.size(); i++) {
					array[i] = new IntWritable(list.get(i));
				}
				context.write(new IntWritable(currow), new IntVector(new ArrayWritable(IntWritable.class, array)));
			}
			list.clear();
			currow = row;
			list.add(key.getSecond().get());
			list.add(values.iterator().next().get());
		} else {
			list.add(key.getSecond().get());
			list.add(values.iterator().next().get());
		}
	}

	@Override
	protected void cleanup(Reducer<IntPair, IntWritable, IntWritable, IntVector>.Context context)
			throws IOException, InterruptedException {
		if (list.size() != 0) {
			// 写出
			IntWritable[] array = new IntWritable[list.size()];
			for (int i = 0; i < list.size(); i++) {
				array[i] = new IntWritable(list.get(i));
			}
			context.write(new IntWritable(currow), new IntVector(new ArrayWritable(IntWritable.class, array)));
		}
	}

}

public class MatrixTransform2 extends Configured implements Tool, Constant {

	public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
		@Override
		public int getPartition(IntPair key, IntWritable value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
		}
	}

	// @Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Transform");

		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setReducerClass(TransformReducer2.class);
		job.setMapperClass(TransformMapper2.class);
		job.setPartitionerClass(FirstPartitioner.class);

		// job.setReducerClass(TestReducer.class);

		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntVector.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MatrixTransform2(), args);
		System.exit(exitCode);
	}
}
