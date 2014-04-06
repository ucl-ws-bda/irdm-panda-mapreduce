package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import uk.ac.ucl.panda.applications.demo.DemoHTMLParser;
import uk.ac.ucl.panda.indexing.io.TrecDoc;
import uk.ac.ucl.panda.utility.io.Config;
import uk.ac.ucl.panda.utility.parser.HTMLParser;
import uk.ac.ucl.panda.utility.structure.Document;
import uk.ac.ucl.panda.utility.structure.Field;

public class IndexRecordReader extends RecordReader<Text, Text> {

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

		key = new Text();
		value = new Text();
		
		filename = path.toString();
		td = new TrecDoc(filename);
		Properties props = new Properties();
		props.setProperty("doc.maker.forever", "false");
		Config conf = new Config(props);
		td.setConfig(conf);

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