package uk.ac.ucl.panda.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ArrayListWritable<T extends Writable> extends ArrayList<T>
		implements Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ArrayListWritable() {
		super();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		int size = in.readInt();
		if (size == 0)
			return;
		String className = in.readUTF();
		T obj;

		try {
			Class<T> c = (Class<T>) Class.forName(className);
			for (int i = 0; i < size; i++) {
				obj = (T) c.newInstance();
				obj.readFields(in);
				this.add(obj);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(size());
		if (size() == 0) {
			return;
		}
		out.writeUTF(get(0).getClass().getCanonicalName());
		for (T obj : this) {
			if (obj == null) {
				throw new IOException("Tried writing null!");
			}
			obj.write(out);
		}
	}
}
