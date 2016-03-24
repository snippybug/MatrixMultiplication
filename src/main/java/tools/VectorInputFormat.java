package tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public	class	VectorInputFormat
extends	FileInputFormat<IntWritable,	IntVector>	{
	@Override
	public	RecordReader<IntWritable,	IntVector>	createRecordReader(
			InputSplit	split,	TaskAttemptContext	context)	throws	IOException,
			InterruptedException	{
	VectorRecordReader	reader	=	new	VectorRecordReader();
	reader.initialize(split,	context);
	return	reader;
	}
}

class VectorRecordReader extends RecordReader<IntWritable, IntVector>{

	private LineRecordReader reader = new LineRecordReader();
	IntWritable key;
	IntVector value;
	
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public IntVector getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(reader.nextKeyValue() == false){
			return false;
		}else{
			Text text= reader.getCurrentValue();
			
			String line = text.toString();
			Scanner scan = new Scanner(line);
			int num = scan.nextInt();			// A的列向量元素个数
			// 读入向量
			ArrayList<Integer> list = new ArrayList<Integer>();
			while(scan.hasNextInt()){
				list.add(scan.nextInt());
			}
			scan.close();
			IntWritable[] array = new IntWritable[list.size()];
			for(int i=0;i<array.length;i++){
				array[i] = new IntWritable(list.get(i));
			}
			
			key = new IntWritable(num);
			value = new IntVector(new ArrayWritable(IntWritable.class, array));
			return true;
		}
	}
	
}