package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntVector implements WritableComparable<IntVector>{
	private ArrayWritable vec;
	public IntVector(){
		vec = new ArrayWritable(IntWritable.class);
	}
	public IntVector(ArrayWritable vec){
		this.vec = vec;
	}
	public ArrayWritable getVector(){
		return vec;
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		vec.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		vec.write(out);
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
		if(o instanceof IntVector){
			IntVector v = (IntVector) o;
			return vec.equals(v.getVector());
		}
		return false;
	}
	@Override
	public int hashCode() {
		return vec.hashCode();
	}
	public int compareTo(IntVector o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
