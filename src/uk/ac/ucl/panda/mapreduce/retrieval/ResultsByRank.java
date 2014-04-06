package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;

public class ResultsByRank extends Reducer<LongWritable, ScoreDocPair, LongWritable, ArrayWritable> {

	public void reduce(LongWritable topicId, Iterable<ScoreDocPair> docs, Context context)
			throws IOException, InterruptedException {
		ArrayWritable results = new ArrayWritable(ScoreDocPair.class);
		int maxResults = context.getConfiguration().getInt("maxResults", 100);
		List<ScoreDocPair> sortedDocs = new ArrayList<ScoreDocPair>();
		for (ScoreDocPair doc: docs) { 
			sortedDocs.add(doc); 				
		}
		Collections.sort(sortedDocs);
		context.getCounter(RetrievalStats.RetrievedDocuments).increment(sortedDocs.size());
		results.set(sortedDocs.subList(0, Math.min(maxResults, sortedDocs.size())).toArray(new ScoreDocPair[1]));
		context.write(topicId, results);
	}
}