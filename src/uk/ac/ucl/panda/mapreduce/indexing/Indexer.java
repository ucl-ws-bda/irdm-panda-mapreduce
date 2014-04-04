package uk.ac.ucl.panda.mapreduce.indexing;


import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.ucl.panda.applications.demo.DemoHTMLParser;
import uk.ac.ucl.panda.indexing.io.TrecDoc;
import uk.ac.ucl.panda.mapreduce.io.Index;
import uk.ac.ucl.panda.mapreduce.io.PairOfStringInt;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.io.Stemmer;
import uk.ac.ucl.panda.mapreduce.util.ObjectFrequencyDistribution;
import uk.ac.ucl.panda.mapreduce.util.Pair;
import uk.ac.ucl.panda.utility.io.Config;
import uk.ac.ucl.panda.utility.parser.HTMLParser;
import uk.ac.ucl.panda.utility.structure.Document;
import uk.ac.ucl.panda.utility.structure.Field;

public class Indexer extends Configured implements Tool {

	static final String DOCIDFIELDNAME = "docname";
	static final String TERMVECTORFIELDNAME = "body";

	public static class WordMapper extends Mapper<Text, Text, Text, PairOfStringInt> {
		private final Text WORD = new Text();
		private final ObjectFrequencyDistribution<String> DISTRIBUTION = new ObjectFrequencyDistribution<String>();

		private MultipleOutputs<Text, LongWritable> mos;
		
		public void setup(Context context) {
		 		mos = new MultipleOutputs(context);
		}
		 
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String doc = value.toString();
			StringTokenizer terms = new StringTokenizer(doc);
			DISTRIBUTION.clear();

			// get tf
			while (terms.hasMoreTokens()) {
				DISTRIBUTION.increment(Stemmer.stem(terms.nextToken()));
			}

			long numTerms = 0;
			// emit word and posting
			for (Pair<String, Integer> posting : DISTRIBUTION) {
				WORD.set(posting.getLeftElement());
				context.write(
						WORD,
						new PairOfStringInt(key.toString(), posting
								.getRightElement()));
				numTerms += posting.getRightElement();
			}			
			context.getCounter(IndexMeta.CollectionLength).increment(numTerms);
			context.getCounter(IndexMeta.NumberOfDocuments).increment(1);
			mos.write(Index.docIndexDir, key, new LongWritable(numTerms));
		}		
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
        }
	}


	public static class SumReducer	extends	Reducer<Text, PairOfStringInt, Text, PostingWritable> {

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

	public static class PandaInputFormat extends FileInputFormat<Text, Text> {

		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return new PandaRecordReader();
		}

	}

	public static class PandaRecordReader extends RecordReader<Text, Text> {

		TrecDoc td;
		Text key;
		Text value;
		String filename;

		@Override
		public void close() throws IOException {
			// NOP- right now handled by Document
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) arg0;
			Path path = fileSplit.getPath();

			key = new Text();
			value = new Text();
			
			filename = path.toString();
			td = new TrecDoc(filename);
			Properties props = new Properties();
			props.setProperty("doc.maker.forever", "false");
			Config conf = new Config(props);
			try {
				td.setConfig(conf);
				HTMLParser htmlParser = (HTMLParser) new DemoHTMLParser();
				td.setHTMLParser(htmlParser);
				context.getCounter(IndexMeta.IndexedFiles).increment(1);
			} catch (IOException e) {
				// Trying to read a non-indexable archive. Ignore
				context.getCounter(IndexMeta.IgnoredFiles).increment(1);
				// Flags nextKeyValue to not try and output anything
				filename = null;
			}
			context.getCounter(IndexMeta.NumberOfFiles).increment(1);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// Not a valid index file. Ignore.
			if (filename == null) {
				return false;
			}
			
			Document doc = null;
			try {
				doc = td.makeDocument(filename);
			} catch (Exception e) {
				return false;
			}
			if (doc == null) {
				return false;
			}
			// get the document name and the terms
			Field docNoField = doc.getField(Indexer.DOCIDFIELDNAME);
			Field termVectorField = doc.getField(Indexer.TERMVECTORFIELDNAME);
			String docNo = docNoField.stringValue();
			String termVector = termVectorField.stringValue();

			// update the key-value pair
			key.set(docNo);
			value.set(termVector);

			return true;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Job job = Job.getInstance();
		job.setJobName("Indexing");
		
		job.setInputFormatClass(PandaInputFormat.class);
		
		PandaInputFormat.setInputPaths(job, in);
		PandaInputFormat.setInputDirRecursive(job, true);
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
		long ignoredFiles = job.getCounters().findCounter(IndexMeta.IgnoredFiles).getValue();
		Index.writeMetaIndex(collectionLength, numberOfDocuments, out);
		System.out.println("Documents processed: " + numberOfDocuments);
		System.out.println("Collection length: " + collectionLength);
		System.out.println("Ignored files: " + ignoredFiles);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Indexer(),
				args);
		System.exit(res);
	}
}
