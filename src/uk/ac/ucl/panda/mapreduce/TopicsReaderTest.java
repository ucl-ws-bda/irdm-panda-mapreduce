package uk.ac.ucl.panda.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import uk.ac.ucl.panda.mapreduce.retrieval.TopicInputFormat;

import junit.framework.TestCase;

public class TopicsReaderTest extends TestCase {

	@Test
	public void testReadTopics() throws IOException, InterruptedException {
		// DISCLAIMER: untested or compiled
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");

		File testFile = new File("/home/ib/irdm/Data/topics.txt");
		FileSplit split = new FileSplit(
		       new Path(testFile.toURI()), 0, 
		       (long)testFile.length(), null); 

		TopicInputFormat inputFormat = new TopicInputFormat();
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		RecordReader<LongWritable, Text> reader = inputFormat.createRecordReader(split, context);
		reader.initialize(split, context);
		
		int num_topics = 0;
		while (reader.nextKeyValue()) {
			num_topics += 1;
		}
		assertEquals(num_topics, 50);
		assertEquals(new LongWritable(450), reader.getCurrentKey());
		assertEquals(new Text("KING HUSSEIN, PEACE"), reader.getCurrentValue());
	}
}
