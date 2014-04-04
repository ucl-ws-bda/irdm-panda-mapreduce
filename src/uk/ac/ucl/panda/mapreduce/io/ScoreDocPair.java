package uk.ac.ucl.panda.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ScoreDocPair implements WritableComparable<ScoreDocPair> {

	private Text docId;
	private DoubleWritable score;
	
	public ScoreDocPair() {
		this(new Text("<unset>"), new DoubleWritable(0));
	}
	
	public ScoreDocPair(Text docId, DoubleWritable score) {
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

	public Text getDocId() {
		return docId;
	}

	public void setDocId(Text docId) {
		this.docId = docId;
	}

	public DoubleWritable getScore() {
		return score;
	}

	public void setScore(DoubleWritable score) {
		this.score = score;
	}
	
	@Override
	public int compareTo(ScoreDocPair other) {
		return this.score.compareTo(other.score);
	}
}
