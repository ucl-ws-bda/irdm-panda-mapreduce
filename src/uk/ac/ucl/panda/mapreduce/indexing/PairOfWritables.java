package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairOfWritables<T1 extends Writable, T2 extends Writable>
		implements Writable {

	private Writable leftElement;
	private Writable rightElement;

	public PairOfWritables(Writable left, Writable right) {
		leftElement = left;
		rightElement = right;
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

	public Writable getLeftElement() {
		return leftElement;
	}

	public void setLeftElement(Writable leftElement) {
		this.leftElement = leftElement;
	}

	public Writable getRightElement() {
		return rightElement;
	}

	public void setRightElement(Writable rightElement) {
		this.rightElement = rightElement;
	}

}
