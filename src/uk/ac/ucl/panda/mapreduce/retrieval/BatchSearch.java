package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.Index;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;
import uk.ac.ucl.panda.mapreduce.io.Stemmer;
import uk.ac.ucl.panda.retrieval.models.ModelParser;

public class BatchSearch extends Configured implements Tool {

	public static class IndexLookup extends Mapper<LongWritable, Text, LongWritable, ScoreDocPair> {

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

	public static class ResultsByRank extends Reducer<LongWritable, ScoreDocPair, LongWritable, ArrayWritable> {

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

	private static final Logger logger = Logger.getLogger(BatchSearch.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			logger.error("Usage: topicsDir outputDir indexDir [model] [maxResults].");
			return -1;
		}

		String topicsPath = args[0];
		String outputPath = args[1];
		String indexPath = args[2];
		
		int model = 2;
		if (args.length >= 4) {
			model = Integer.parseInt(args[3]);	
		}		
		if (model < 0 || model >= ModelParser.getNumberOfModels()) {
			logger.error("Invalid model number. Must be between 0 and " + ModelParser.getNumberOfModels());
			return -2;
		}

		int maxResults = 100;
		if (args.length >= 5) {
			maxResults = Integer.parseInt(args[4]);	
		}
				
		Job job = Job.getInstance();
		job.getConfiguration().set("indexDir", indexPath);
		// TODO: validate model number
		job.getConfiguration().setInt("model", model);
		job.getConfiguration().setInt("maxResults", maxResults);
		
		Map<String, Long> metaIndex = Index.readMetaIndex(new Path(indexPath));
		job.getConfiguration().setDouble("cl", metaIndex.get("CollectionLength"));
		job.getConfiguration().setLong("docNum", metaIndex.get("NumberOfDocuments"));
		job.getConfiguration().setDouble("avgDL", metaIndex.get("CollectionLength") / metaIndex.get("NumberOfDocuments"));
		System.out.println("Collection length: " + metaIndex.get("CollectionLength"));		
		System.out.println("Number of documents: " + metaIndex.get("NumberOfDocuments"));		
		System.out.println("Average document length: " +  metaIndex.get("CollectionLength") / metaIndex.get("NumberOfDocuments"));		
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), job.getConfiguration()).delete(outputDir, true);
				
		job.setJobName("Batch Search");
		//job.setNumReduceTasks(reduceTasks);
		
		job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ScoreDocPair.class);
        job.setOutputValueClass(ArrayWritable.class);
 
        job.setMapperClass(IndexLookup.class); 
        job.setReducerClass(ResultsByRank.class);  
 
        job.setInputFormatClass(TopicInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(topicsPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
        job.setJarByClass(BatchSearch.class);
        job.waitForCompletion(true);
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BatchSearch(), args);
		System.exit(res);
	}
}