package uk.ac.ucl.panda.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class PostingWritable implements Writable {

	private LongWritable collectionTermFrequency;
	private LongWritable documentFrequency;
	private MapWritable observations;
	
	public PostingWritable() {
		this(new LongWritable(0), new LongWritable(0));
	}
	
	public PostingWritable(LongWritable ctf, LongWritable df) {
		collectionTermFrequency = ctf;
		documentFrequency = df;
		observations = new MapWritable();
	}
	
	public void addObservation(LongWritable docId, LongWritable termFrequency, ArrayWritable locations) {
		observations.put(docId, new FrequencyLocationsPair(termFrequency, locations));
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		collectionTermFrequency.readFields(in);
		documentFrequency.readFields(in);
		observations.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		collectionTermFrequency.write(out);
		documentFrequency.write(out);
		observations.write(out);		
	}

	public LongWritable getCollectionTermFrequency() {
		return collectionTermFrequency;
	}

	public LongWritable getDocumentFrequency() {
		return documentFrequency;
	}

	public MapWritable getObservations() {
		return observations;
	}

	public void setCollectionTermFrequency(LongWritable collectionTermFrequency) {
		this.collectionTermFrequency = collectionTermFrequency;
	}

	public void setDocumentFrequency(LongWritable documentFrequency) {
		this.documentFrequency = documentFrequency;
	}

	public void setObservations(MapWritable observations) {
		this.observations = observations;
	}
}
