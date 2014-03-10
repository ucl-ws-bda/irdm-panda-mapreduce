package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import uk.ac.ucl.panda.applications.demo.DemoHTMLParser;
import uk.ac.ucl.panda.indexing.io.TrecDoc;
import uk.ac.ucl.panda.mapreduce.indexing.Indexer.PandaRecordReader;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.utility.io.Config;
import uk.ac.ucl.panda.utility.parser.HTMLParser;
import uk.ac.ucl.panda.utility.structure.Document;
import uk.ac.ucl.panda.utility.structure.Field;

public class Indexer {

  private static final String DOCIDFIELDNAME = "docname";
  private static final String TERMVECTORFIELDNAME = "body";

	public class WordMapper extends Mapper<Text, Text, Text, PairOfStringInt> {
	    private final static Text WORD = new Text();
	    private final static ObjectFrequencyDistribution<String> DISTRIBUTION =
			new ObjectFrequencyDistribution<String>();

	    public void map(Text key, Text value, Context context)
	            throws IOException, InterruptedException {
	          String doc = value.toString();
	          StringTokenizer terms = new StringTokenizer(doc);
	          DISTRIBUTION.clear();

	          // get tf
	          while (terms.hasMoreTokens()) {
	            DISTRIBUTION.increment(terms.nextToken());
	          }

	          // emit word and posting
	          for (Pair<String, Integer> posting : DISTRIBUTION) {
	            WORD.set(posting.getLeftElement());
	            context.write(WORD,
	                new PairOfStringInt(key.toString(), posting.getRightElement()));
	          }
	        }
	}
	
	public class SumReducer extends Reducer<Text, PairOfStringInt, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfStringInt>>> {
		private final static IntWritable DF = new IntWritable();

	    public void reduce(Text key, Iterable<PairOfStringInt> values, Context context)
	        throws IOException, InterruptedException {
	      ArrayListWritable<PairOfStringInt> postings = new ArrayListWritable<PairOfStringInt>();

	      // get df
	      int df = 0;
	      for (PairOfStringInt posting : values) {
			//TODO: is clone() necessary here?
	        postings.add(posting);
	        df++;
	      }
	      DF.set(df);

	      // emit word, df, and postings
	      context.write(key,
	          new PairOfWritables<IntWritable, ArrayListWritable<PairOfStringInt>>(DF,
	              postings));
	    }
	}

	public class PandaInputFormat extends FileInputFormat<Text, Text>  {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
      return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
        TaskAttemptContext arg1) throws IOException, InterruptedException {
      return new PandaRecordReader();
    }

  }

	public class PandaRecordReader extends RecordReader<Text, Text> {

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
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) arg0;
      Path path = fileSplit.getPath();

      // set up key and value
      key = new Text();
      value = new Text();

      filename = path.getName();

      td = new TrecDoc();

      // set the conf
      Properties props = new Properties();
      Config conf = new Config(props);
      td.setConfig(conf);

      // set the parser
      HTMLParser htmlParser = (HTMLParser) new DemoHTMLParser();
      td.setHTMLParser(htmlParser);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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
      Field docNoField = doc.getField(DOCIDFIELDNAME);
      Field termVectorField = doc.getField(TERMVECTORFIELDNAME);
      String docNo = docNoField.stringValue();
      String termVector = termVectorField.stringValue();

      // update the key-value pair
      key.set(docNo);
      value.set(termVector);

      return true;
    }

  }
}
