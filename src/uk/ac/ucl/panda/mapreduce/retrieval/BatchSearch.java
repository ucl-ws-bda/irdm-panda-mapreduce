package uk.ac.ucl.panda.mapreduce.retrieval;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;
import uk.ac.ucl.panda.mapreduce.util.Index;
import uk.ac.ucl.panda.retrieval.models.ModelParser;

public class BatchSearch extends Configured implements Tool {

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