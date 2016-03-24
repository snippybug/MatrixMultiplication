package localhost5;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import tools.IntPair;
import tools.IntVector;
import tools.VectorInputFormat;

// 基于Hadoop的矩阵乘法程序
// 外积法
// 输入格式： A的列向量分量 [B的行向量]
// 两个文件输入

class MatrixMapper3 extends Mapper<LongWritable, Text, IntPair, IntWritable>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int x = scan.nextInt();
		int p = scan.nextInt();
		while(scan.hasNextInt()){
			context.write(new IntPair(x, scan.nextInt()), new IntWritable(scan.nextInt()*p));
		}
		scan.close();
	}
}

class MatrixReducer3 extends Reducer<IntPair, IntWritable, IntPair, IntWritable>
implements Constant{
@Override
public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException{
		Iterator<IntWritable> it = values.iterator();
//		System.out.print("key="+key.getFirst().get()+" "+key.getSecond().get());
		int sum = 0;
	//	System.out.print(", val=");
		while(it.hasNext()){
			int val = it.next().get();
			sum += val;
			//System.out.print(" "+val);
		}
		//System.out.println();
		context.write(key, new IntWritable(sum));
	}
}

public class MatrixMultiplication3 extends Configured implements Tool, Constant{
	
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 3){
			System.err.printf("Usage: %s [generic options] <input1>  <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Multiplication");
		
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MatrixMapper3.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,MatrixMapper3.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(MatrixReducer3.class);
		job.setMapperClass(MatrixMapper3.class);
		//job.setReducerClass(TestReducer.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication3(), args);
		System.exit(exitCode);
	}
}
