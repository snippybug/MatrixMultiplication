package localhost2;

import java.io.IOException;
import java.util.Iterator;

import tools.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class MergeMapper extends Mapper<IntPair, IntWritable, IntPair, IntWritable>{
	@Override
	protected void map(IntPair key, IntWritable value, Context context)
		throws IOException, InterruptedException{
		context.write(key, value);
	}
}

class MergeReducer extends Reducer<IntPair, IntWritable, IntPair, IntWritable>{
		@Override
		protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
			Iterator<IntWritable> it = values.iterator();
			int sum = 0;
			while(it.hasNext()){
				sum += it.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
}

public class MatrixMerge extends Configured implements Tool, Constant{

	public int run(String[] args)		throws Exception{
		if(args.length != 2){
			System.err.printf("Usage: %s <input directory> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Matrix Merge");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MatrixMerge(), args);
		System.exit(exitCode);
	}
}