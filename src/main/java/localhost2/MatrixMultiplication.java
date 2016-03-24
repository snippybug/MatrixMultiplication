package localhost2;

// 方法二的要点是以矩阵A的每一列乘以矩阵矩阵B的每一行，然后将各个子矩阵的结果相加

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.*;

class MatrixAMapper extends Mapper<LongWritable, Text, IntWritable, IntTriple>
implements Constant{
	@Override
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		String line = value.toString();
		String[] strs = line.split("\t");
		assert(strs.length == 3);
		//WARNING: if input data is separated by other delims, parseInt will have an exception
		int[] values= {Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Integer.parseInt(strs[2])};
		// values[0]、values[1]和values[2]分别表示行数、列数和数值
		context.write(new IntWritable(values[1]), new IntTriple(0, values[0], values[2]));	// 以列数为键值，IntTriple的第一个参数0表示这项来自矩阵A
	}
}

class MatrixBMapper extends Mapper<LongWritable, Text, IntWritable, IntTriple>
implements Constant{
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		String[] strs = line.split("\t");
		assert(strs.length == 3);
		int[] values= {Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Integer.parseInt(strs[2])};
		// 与MatrixAMapper类似，只是以行数为键值
		context.write(new IntWritable(values[0]), new IntTriple(1, values[1], values[2]));
	}
}

class MatrixReducer extends Reducer<IntWritable, IntTriple, IntPair, IntWritable>
implements Constant{
	private class Pair{
		public int a;
		public int b;
		public Pair(int a, int b){
			this.a = a;
			this.b = b;
		}
		@Override
		public String toString(){
			return Integer.toString(a)+"\t"+Integer.toString(b);
		}
	}
	private MultipleOutputs<IntPair, IntWritable> multipleOutputs;
	@Override
	protected void setup(Context context)
		throws IOException, InterruptedException{
		multipleOutputs = new MultipleOutputs<IntPair, IntWritable>(context);
	}
	@Override
	public void reduce(IntWritable key, Iterable<IntTriple> values, Context context)
		throws IOException, InterruptedException{
		Iterator<IntTriple> it = values.iterator();
		List<Pair> Alist = new ArrayList<Pair>();		// 保存来自A的列向量
		List<Pair> Blist = new ArrayList<Pair>();		// 保存来自B的行向量
		while(it.hasNext()){
			IntTriple temp = it.next();
			if(temp.getFirst().get() == 0){					// 来自A
				System.out.println("Come from A, temp="+temp);
				Alist.add(new Pair(temp.getSecond().get(), temp.getThird().get()));
			}else if(temp.getFirst().get() == 1){		// 来自B
				System.out.println("Come from B, temp="+temp);
				Blist.add(new Pair(temp.getSecond().get(), temp.getThird().get()));
			}else{
				// 无效
			}
		}
		System.out.println("Alist="+Alist+", Blist="+Blist);
		for(int i=0;i<Alist.size();i++){
			for(int j=0;j<Blist.size();j++){
				System.out.println("i="+i+", j="+j);
				int a = Alist.get(i).a;
				int b = Blist.get(j).a;
				int c =Alist.get(i).b * Blist.get(j).b;
				System.out.println("a="+Alist.get(i)+", b="+Blist.get(j)+", c="+c);
				multipleOutputs.write(new IntPair(a, b), new IntWritable(c), key.toString());
				//context.write(NullWritable.get(), new IntTriple(a, b, c));
			}
		}
	}
	@Override
	protected void cleanup(Context context)
		throws IOException, InterruptedException{
		multipleOutputs.close();
	}
}

public class MatrixMultiplication extends Configured implements Tool, Constant{

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
		//job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntTriple.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMultiplication(), args);
		System.exit(exitCode);
	}
}
