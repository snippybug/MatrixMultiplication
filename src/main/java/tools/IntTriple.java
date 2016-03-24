package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntTriple implements WritableComparable<IntTriple>{
	private IntPair pair;
	private IntWritable key;
	public IntTriple(){
		pair = new IntPair();
		key = new IntWritable();
	}
	public IntTriple(int a, int b, int c){
		key = new IntWritable(a);
		pair = new IntPair(b, c);
	}
	public IntWritable getFirst(){
		return key;
	}
	public IntWritable getSecond(){
		return pair.getFirst();
	}
	public IntWritable getThird(){
		return pair.getSecond();
	}
	public IntPair getPair(){
		return pair;
	}
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		pair.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		key.write(out);
		pair.write(out);
	}
	public int compareTo(IntTriple it) {
		int cmp = key.compareTo(it.getFirst());
		if(cmp != 0){
			return cmp;
		}
		return pair.compareTo(it.pair);
	}
	
	@Override
	public int hashCode(){
		return pair.hashCode();
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof IntTriple){
			IntTriple ip = (IntTriple) o;
			return pair.equals(ip.getPair()) && key.equals(ip.getFirst());
		}
		return false;
	}
	@Override
	public String toString(){
		return key.toString() + ' ' + pair.toString();
	}
}
