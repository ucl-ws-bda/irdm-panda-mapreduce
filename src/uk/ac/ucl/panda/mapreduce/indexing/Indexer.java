package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.io.TermWritable;
import uk.ac.ucl.panda.utility.io.Config;
import uk.ac.ucl.panda.utility.parser.HTMLParser;
import uk.ac.ucl.panda.utility.structure.Document;
import uk.ac.ucl.panda.utility.structure.Field;

public class Indexer {

  private static final String DOCIDFIELDNAME = "docname";
  private static final String TERMVECTORFIELDNAME = "body";

	public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
		
	}
	
	public class SumReducer extends Reducer<Text, Text, Text, PostingWritable> {

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
