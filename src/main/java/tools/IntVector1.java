package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntVector1 implements WritableComparable<IntVector1>{
	private ArrayWritable vec;
	private IntWritable val;
	public IntVector1(){
		vec = new ArrayWritable(IntWritable.class);
		val = new IntWritable(-1);
	}
	public IntVector1(ArrayWritable vec, IntWritable val){
		this.vec = vec;
		this.val = val;
	}
	public ArrayWritable getVector(){
		return vec;
	}
	public IntWritable getValue(){
		return val;
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		vec.readFields(in);
		val.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		vec.write(out);
		val.write(out);
	}

	public int compareTo(IntVector1 o) {
		return val.compareTo(o.getValue());
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
		if(o instanceof IntVector1){
			IntVector1 v = (IntVector1) o;
			if(val.equals(v.getValue()) && vec.equals(v.getVector())){
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
