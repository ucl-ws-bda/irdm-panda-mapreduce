package uk.ac.ucl.panda.mapreduce.indexing;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.PairOfStringInt;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.util.Index;

public class Indexer extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(Indexer.class);

	static final String DOCIDFIELDNAME = "docname";
	static final String TERMVECTORFIELDNAME = "body";

	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Job job = Job.getInstance();
		job.setInputFormatClass(PandaInputFormat.class);

		PandaInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setOutputFormatClass(MapFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairOfStringInt.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PostingWritable.class);
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);

		// Output in SequenceFile since docIds arrive out of order which is required
		// for MapFiles. We will post-process merge these into a MapFile after indexing.
		MultipleOutputs.addNamedOutput(job, Index.docIndexDir, SequenceFileOutputFormat.class,
									   Text.class, LongWritable.class);
		
		job.setJarByClass(Indexer.class);
		
		FileSystem.get(out.toUri(), job.getConfiguration()).delete(out, true);

		job.waitForCompletion(true);
		
		long numberOfDocuments = job.getCounters().findCounter(IndexMeta.NumberOfDocuments).getValue();
		long collectionLength = job.getCounters().findCounter(IndexMeta.CollectionLength).getValue();
		Index.writeMetaIndex(collectionLength, numberOfDocuments, out);
		logger.info("Documents processed: " + numberOfDocuments);
		logger.info("Collection length: " + collectionLength);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Indexer(),
				args);
		System.exit(res);
	}
}
