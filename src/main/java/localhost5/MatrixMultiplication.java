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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Constant;
import tools.IntPair;

// 基于Hadoop的矩阵乘法程序
// 外积法
// 输入格式：[A的列向量的元素个数] [A的列向量] [B的行向量]
// 一个文件输入

class MatrixMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int num = scan.nextInt()*2;			// A的列向量元素个数
		// 读入A的列向量
		LinkedList<Integer> listA = new LinkedList<Integer>();
		for(int i=0;i<num;i++){
			listA.add(scan.nextInt());
		}
		// 剩下的是B的行向量
		LinkedList<Integer> listB = new LinkedList<Integer>();
		while(scan.hasNextInt()){
			listB.add(scan.nextInt());
		}
		scan.close();
		for(int i=0;i<num;i+=2){
			int x = listA.get(i);				// 行号
			for(int j=0;j<listB.size();j+=2){
				int y = listB.get(j);			// 列号
				context.write(new IntPair(x, y), new IntWritable(listA.get(i+1)*listB.get(j+1)));
			}
		}
	}
}

class MatrixReducer extends Reducer<IntPair, IntWritable, IntPair, IntWritable>
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

public class MatrixMultiplication extends Configured implements Tool, Constant{
	
	//@Override
	public int run(String[] args)		throws Exception{
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Multiplication");
		
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setReducerClass(MatrixReducer.class);
		job.setMapperClass(MatrixMapper.class);
		//job.setReducerClass(TestReducer.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
