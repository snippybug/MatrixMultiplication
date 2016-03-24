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
// 输入格式：[A的列向量的元素个数] [A的列向量] [B的行向量]
// 两个文件输入

class MatrixMapper2 extends Mapper<LongWritable, Text, IntPair, IntWritable>{
@Override
protected void map(LongWritable key, Text text, Context context)
	throws IOException, InterruptedException{
	System.out.println("Map Start--------------");
		String line = text.toString();
		Scanner scan = new Scanner(line);
		int num = scan.nextInt()*2;			// A的列向量元素个数
		// 读入A的列向量
		LinkedList<Integer> listA = new LinkedList<Integer>();
		for(int i=0;i<num;i++){
			listA.add(scan.nextInt());
		}
		System.out.println("Map: After reading vector A --------------");
		// 剩下的是B的行向量
		LinkedList<Integer> listB = new LinkedList<Integer>();
		while(scan.hasNextInt()){
			listB.add(scan.nextInt());
		}
		scan.close();
		System.out.println("Map: After reading vector B --------------");
		
		for(int i=0;i<num;i+=2){
			int x = listA.get(i);				// 行号
			for(int j=0;j<listB.size();j+=2){
				int y = listB.get(j);			// 列号
				context.write(new IntPair(x, y), new IntWritable(listA.get(i+1)*listB.get(j+1)));
			}
		}
	/*
	int num = key.get();
	IntWritable[] array = (IntWritable[])values.getVector().toArray();
	for(int i=0;i<num*2;i+=2){
		int x = array[i].get();
		for(int j=num*2;j<array.length;j+=2){
			int y = array[j].get();
			context.write(new IntPair(x, y), new IntWritable(array[x+1].get()*array[y+1].get()));
		}
	}
	*/
	System.out.println("Map End--------------");
	}
}

class MatrixReducer2 extends Reducer<IntPair, IntWritable, IntPair, IntWritable>
implements Constant{
@Override
public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException{
	//System.out.println("Reduce Start--------------");
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
		//System.out.println("Reduce End--------------");
	}
}

public class MatrixMultiplication2 extends Configured implements Tool, Constant{
	
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
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MatrixMapper2.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,MatrixMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(MatrixReducer2.class);
		job.setMapperClass(MatrixMapper2.class);
		job.setCombinerClass(MatrixReducer2.class);
		//job.setReducerClass(TestReducer.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication2(), args);
		System.exit(exitCode);
	}
}
