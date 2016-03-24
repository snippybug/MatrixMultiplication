package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntQuad implements WritableComparable<IntQuad> {
	private IntPair p1;
	private IntPair p2;

	public IntQuad(){
		p1 = new IntPair();
		p2 = new IntPair();
	}
	
	public IntQuad(int a, int b, int c, int d) {
		p1 = new IntPair(a, b);
		p2 = new IntPair(c, d);
	}

	public IntPair getP1() {
		return p1;
	}

	public IntPair getP2() {
		return p2;
	}

	public IntWritable getA() {
		return p1.getFirst();
	}

	public IntWritable getB() {
		return p1.getSecond();
	}

	public IntWritable getC() {
		return p2.getFirst();
	}

	public IntWritable getD() {
		return p2.getSecond();
	}

	public void write(DataOutput out) throws IOException {
		p1.write(out);
		p2.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		p1.readFields(in);
		p2.readFields(in);
	}

	public int compareTo(IntQuad o) {
		int cmp = p1.compareTo(o.getP1());
		if (cmp == 0) {
			return p2.compareTo(o.getP2());
		} else {
			return cmp;
		}
	}

	@Override
	public String toString() {
		return p1.toString()+" "+p2.toString();
	}

	
}