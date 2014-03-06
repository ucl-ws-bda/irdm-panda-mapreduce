package uk.ac.ucl.panda.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class FrequencyLocationsPair implements Writable {

	private final LongWritable termFrequency;
	private final ArrayWritable locations;
	
	public FrequencyLocationsPair() {
		this(new LongWritable(), new ArrayWritable(LongWritable.class));
	}
	
	public FrequencyLocationsPair(LongWritable termFrequency, ArrayWritable locations) {
		this.termFrequency = termFrequency;
		this.locations = locations;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		termFrequency.write(out);
		locations.write(out);
		
	}

	public LongWritable getTermFrequency() {
		return termFrequency;
	}

	public ArrayWritable getLocations() {
		return locations;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		termFrequency.readFields(in);
		locations.readFields(in);		
	}

}
