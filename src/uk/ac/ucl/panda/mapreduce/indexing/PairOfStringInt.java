package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PairOfStringInt implements Writable {

	private Text leftElement;
	private IntWritable rightElement;

	public PairOfStringInt(String left, Integer right) {
		leftElement.set(left);
		rightElement.set(right);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		leftElement.readFields(in);
		rightElement.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		leftElement.write(out);
		rightElement.write(out);
	}

	public Text getLeftElement() {
		return leftElement;
	}

	public void setLeftElement(Text leftElement) {
		this.leftElement = leftElement;
	}

	public IntWritable getRightElement() {
		return rightElement;
	}

	public void setRightElement(IntWritable rightElement) {
		this.rightElement = rightElement;
	}

}
