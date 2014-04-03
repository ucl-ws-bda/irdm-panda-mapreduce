package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.File;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.Index;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;
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
				queryTerms.add(tokenizer.nextToken());
			}			

			String indexDir = context.getConfiguration().get("indexDir");
			Map<String, Integer> metaIndex = Index.readMetaIndex(new File(indexDir));
			double cl = metaIndex.get("CL");
			double avgDL = metaIndex.get("avgDL");
			int docNum = metaIndex.get("docNum");
			
			Map<LongWritable, Double> scores = new HashMap<LongWritable, Double>();
			
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
					double dl = Index.fetchDocumentLength(indexDir, (LongWritable)docId).get();
					double score = model.getscore(tf, df, idf, dl, avgDL, docNum, cl, ctf, qtf);
					if (scores.containsKey(docId)) {
						score += scores.get(docId);
					}
					scores.put((LongWritable)docId, score);
				}
				
			}
			
			for (LongWritable docId: scores.keySet()) {
				scoreDoc = new ScoreDocPair(docId, new DoubleWritable(scores.get(docId)));
				context.write(topicId, scoreDoc);
			}						
		}
	}

	public static class ResultsByRank extends Reducer<LongWritable, ScoreDocPair, LongWritable, ArrayWritable> {

		private ArrayWritable results = new ArrayWritable(ScoreDocPair.class);
		
		public void reduce(LongWritable topicId, Iterable<ScoreDocPair> docs, Context context)
				throws IOException, InterruptedException {
			int maxResults = context.getConfiguration().getInt("maxResults", 100);
			List<ScoreDocPair> sortedDocs = new ArrayList<ScoreDocPair>();
			for (ScoreDocPair doc: docs) { 
				sortedDocs.add(doc); 
			}
			Collections.sort(sortedDocs);
			results.set(sortedDocs.subList(0, maxResults).toArray(new ScoreDocPair[1]));
		}
	}

	private static final Logger logger = Logger.getLogger(BatchSearch.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			logger.error("RTFM.");
			return -1;
		}

		String topicsPath = args[0];
		String outputPath = args[1];
		String indexPath = args[2];
		
		int reduceTasks = Integer.parseInt(args[3]);	
		
		// TODO: args should be 0: topics dir, 1: output dir, 2:index dir, 3: num reducers, 4: model, 5: max results
		Job job = Job.getInstance();
		job.getConfiguration().set("indexDir", indexPath);
		// TODO: validate model number
		job.getConfiguration().setInt("model", Integer.parseInt(args[4]));
		job.getConfiguration().setInt("maxResults", Integer.parseInt(args[5]));
		
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), job.getConfiguration()).delete(outputDir, true);
				
		job.setJobName("Batch Search");
		job.setNumReduceTasks(reduceTasks);
		
        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ScoreDocPair.class);
        job.setOutputValueClass(ArrayWritable.class);
 
        job.setMapperClass(IndexLookup.class); 
        job.setReducerClass(ResultsByRank.class);  
 
        job.setInputFormatClass(TopicInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(topicsPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
        job.setJarByClass(BatchSearch.class);
        job.waitForCompletion(true);
        System.out.println(job.getConfiguration().get("indexDir"));
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