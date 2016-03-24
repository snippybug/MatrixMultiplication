package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair>{
	private IntWritable first;
	private IntWritable second;	
	public IntPair(){
		set(new IntWritable(), new IntWritable());
	}
	public IntPair(IntWritable a, IntWritable b){
		first = a;
		second = b;
	}
	public IntPair(int a, int b){
		set(new IntWritable(a), new IntWritable(b));
	}
	public void set(IntWritable a, IntWritable b){
		first = a;
		second = b;
	}
	public IntWritable getFirst(){
		return first;
	}
	public IntWritable getSecond(){
		return second;
	}
	//@Override
	  public void write(DataOutput out) throws IOException {
	    first.write(out);
	    second.write(out);
	  }

	  //@Override
	  public void readFields(DataInput in) throws IOException {
	    first.readFields(in);
	    second.readFields(in);
	  }
	@Override
	public int hashCode(){
		return first.hashCode() * 163 + second.hashCode();
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof IntPair){
			IntPair ip = (IntPair) o;
			return first.equals(ip.first) && second.equals(ip.second);
		}
		return false;
	}
	@Override
	public String toString(){
		return first.toString() + ' ' + second.toString();
	}
	//@Override
	public int compareTo(IntPair ip){
		int cmp = first.compareTo(ip.first);
		if(cmp != 0){
			return cmp;
		}
		return second.compareTo(ip.second);
	}
}