package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;
import uk.ac.ucl.panda.mapreduce.util.Index;
import uk.ac.ucl.panda.mapreduce.util.Stemmer;
import uk.ac.ucl.panda.retrieval.models.ModelParser;

public class IndexLookup extends Mapper<LongWritable, Text, LongWritable, ScoreDocPair> {

	private ScoreDocPair scoreDoc = new ScoreDocPair();

	@Override
	public void map(LongWritable topicId, Text query, Context context) throws IOException,
			InterruptedException {
					
		int modelNum = context.getConfiguration().getInt("model", 2); // BM25
		ModelParser model = new ModelParser(modelNum);
		
		StringTokenizer tokenizer = new StringTokenizer(query.toString());
		List<String> queryTerms = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			queryTerms.add(Stemmer.stem(tokenizer.nextToken()));
		}			

		String indexDir = context.getConfiguration().get("indexDir");
		double cl = context.getConfiguration().getDouble("cl", 0);
		double avgDL = context.getConfiguration().getDouble("avgDL", 0);
		int docNum = (int)(context.getConfiguration().getLong("docNum", 0));
		
		Map<Text, Double> scores = new HashMap<Text, Double>();
		
		for (String term: queryTerms) {
			int qtf = Collections.frequency(queryTerms, term);
			
			PostingWritable posting = Index.fetchPosting(indexDir, term);
			int ctf = (int)posting.getCollectionTermFrequency().get();
			double df = posting.getDocumentFrequency().get();
			MapWritable observations = posting.getObservations();
			// We add 1 to df to avoid zero-division. This is common practice.
			double idf = Math.log(docNum / (1 + df));
			for (Writable docId: observations.keySet()) {
				int tf = ((IntWritable)observations.get(docId)).get();
				double dl = Index.fetchDocumentLength(indexDir, (Text)docId).get();
				double score = model.getscore(tf, df, idf, dl, avgDL, docNum, cl, ctf, qtf);
				if (scores.containsKey(docId)) {
					score += scores.get(docId);
				}
				scores.put((Text)docId, score);
			}
			
		}
		
		// Note: If no documents are matched then no output is given for this topicId.
		context.getCounter(RetrievalStats.NumberOfQueries).increment(1);
		for (Text docId: scores.keySet()) {
			scoreDoc = new ScoreDocPair(docId, new DoubleWritable(scores.get(docId)));
			context.getCounter(RetrievalStats.TotalScore).increment(scores.get(docId).intValue());
			context.write(topicId, scoreDoc);
			context.getCounter(RetrievalStats.ScoredDocuments).increment(1);
		}						
	}
}