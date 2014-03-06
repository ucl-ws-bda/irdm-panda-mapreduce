package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;

public class BatchSearch extends Configured implements Tool {

	public static class IndexLookup extends Mapper<LongWritable, Text, LongWritable, ScoreDocPair> {
		private LongWritable topicId = new LongWritable();
		private ScoreDocPair scoreDoc = new ScoreDocPair();

		// TODO: can model and other stuff be passed on init?
		
		@Override
		public void map(LongWritable topicId, Text query, Context contex) throws IOException,
				InterruptedException {
			// TODO: calculate the score based on given model
		}
	}

	public static class ResultsByRank extends Reducer<LongWritable, ScoreDocPair, LongWritable, ArrayWritable> {

		private ArrayWritable results = new ArrayWritable(ScoreDocPair.class);
		
		// TODO: can max results be set on init?
		
		public void reduce(LongWritable topicId, Iterable<ScoreDocPair> topDocs, Context context)
				throws IOException, InterruptedException {
			// TODO: not much, should be rank ordered already, maybe trim to only retain top K results
		}
	}

	private static final Logger logger = Logger.getLogger(BatchSearch.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			logger.error("RTFM.");
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		
		int reduceTasks = Integer.parseInt(args[3]);	
		
		// TODO: custom topic reader
		// TODO: get the index and model to mapper somehow, preferably on init
		
		Job job = Job.getInstance();
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), job.getConfiguration()).delete(outputDir, true);
				
		job.setJobName("Batch Search");
		job.setNumReduceTasks(reduceTasks);
		
        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ScoreDocPair.class);
        job.setOutputValueClass(ArrayWritable.class);
 
        job.setMapperClass(IndexLookup.class); 
        job.setReducerClass(ResultsByRank.class);  
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
        job.setJarByClass(BatchSearch.class);
		
        job.submit();
        
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