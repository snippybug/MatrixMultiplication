package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntVector2 implements WritableComparable<IntVector2>{
	private ArrayWritable vec;
	private IntWritable first;
	private IntWritable second;
	public IntVector2(){
		vec = new ArrayWritable(IntWritable.class);
		first = new IntWritable();
		second = new IntWritable();
	}
	public IntVector2(ArrayWritable vec, IntWritable first, IntWritable second){
		this.vec = vec;
		this.first = first;
		this.second = second;
	}
	public ArrayWritable getVector(){
		return vec;
	}
	public IntWritable getFirst(){
		return first;
	}
	public IntWritable getSecond(){
		return second;
	}
	public void setSecond(int second){
		this.second.set(second);
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		vec.readFields(in);
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		vec.write(out);
		first.write(out);
		second.write(out);
	}

	public int compareTo(IntVector2 o) {
		// 优先比较first
		int ret = first.compareTo(o.getFirst());
		if(ret == 0){
			ret = second.compareTo(o.getSecond());
		}
		return ret;
	}
	
	@Override
	public String toString(){
		IntWritable[] array = (IntWritable[])vec.toArray();
		StringBuffer buf = new StringBuffer();
		for(int i=0;i<array.length;i++){
			buf.append(array[i].get());
			buf.append(' ');
		}
		return buf.toString();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof IntVector2){
			IntVector2 v = (IntVector2) o;
			if(first.equals(v.getFirst()) && second.equals(v.getSecond()) && vec.equals(v.getVector())){
				return true;
			}
		}
		return false;
	}
	@Override
	public int hashCode() {
		return vec.hashCode();
	}
	
	
}
