package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.ucl.panda.mapreduce.io.PairOfStringInt;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;

public class SumReducer	extends	Reducer<Text, PairOfStringInt, Text, PostingWritable> {

	public void reduce(Text key, Iterable<PairOfStringInt> values,
			Context context) throws IOException, InterruptedException {
		
		PostingWritable posting = new PostingWritable();
				
		// Calculate document frequency and collection term frequency.
		int df = 0;
		int ctf = 0;
		for (PairOfStringInt observation : values) {
			posting.addObservation(observation.getLeftElement(), observation.getRightElement());
			ctf += observation.getRightElement().get();
			df++;
		}
		posting.setDocumentFrequency(new LongWritable(df));
		posting.setCollectionTermFrequency(new LongWritable(ctf));			
		context.write(key, posting);
	}
}