package uk.ac.ucl.panda.mapreduce.util;

public class Pair<T1, T2> {

	private T1 leftElement;
	private T2 rightElement;

	public Pair(T1 left, T2 right) {
		leftElement = left;
		rightElement = right;
	}

	public T1 getLeftElement() {
		return leftElement;
	}

	public void setLeftElement(T1 leftElement) {
		this.leftElement = leftElement;
	}

	public T2 getRightElement() {
		return rightElement;
	}

	public void setRightElement(T2 rightElement) {
		this.rightElement = rightElement;
	}

}
