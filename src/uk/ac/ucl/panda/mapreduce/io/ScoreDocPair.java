package uk.ac.ucl.panda.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ScoreDocPair implements Writable {

	private LongWritable docId;
	private DoubleWritable score;
	
	public ScoreDocPair() {
		this(new LongWritable(0), new DoubleWritable(0));
	}
	
	public ScoreDocPair(LongWritable docId, DoubleWritable score) {
		this.docId = docId;
		this.score = score;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		docId.write(out);
		score.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		docId.readFields(in);
		score.readFields(in);
	}

	public LongWritable getDocId() {
		return docId;
	}

	public void setDocId(LongWritable docId) {
		this.docId = docId;
	}

	public DoubleWritable getScore() {
		return score;
	}

	public void setScore(DoubleWritable score) {
		this.score = score;
	}

}
