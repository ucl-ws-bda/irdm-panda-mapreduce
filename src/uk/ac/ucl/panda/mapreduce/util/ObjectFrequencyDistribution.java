package uk.ac.ucl.panda.mapreduce.util;

import java.util.HashMap;
import java.util.Iterator;

public class ObjectFrequencyDistribution<T> implements
		Iterable<Pair<T, Integer>> {
	HashMap<T, Integer> backend;

	public ObjectFrequencyDistribution() {
		backend = new HashMap<T, Integer>();
	}

	public void clear() {
		backend.clear();
	}

	public void increment(T key) {
		Integer currentValue = backend.get(key);
		Integer newValue = new Integer(currentValue.intValue() + 1);
		backend.put(key, newValue);
	}

	@Override
	public Iterator<Pair<T, Integer>> iterator() {

		Iterator<Pair<T, Integer>> it = new Iterator<Pair<T, Integer>>() {

			Iterator<T> backendIterator = backend.keySet().iterator();

			@Override
			public boolean hasNext() {
				return backendIterator.hasNext();
			}

			@Override
			public Pair<T, Integer> next() {
				T key = backendIterator.next();
				Integer value = backend.get(key);
				return new Pair<T, Integer>(key, value);
			}

			@Override
			public void remove() {
				throw new NotImplementedException();
			}
		};
		return it;
	}
}
