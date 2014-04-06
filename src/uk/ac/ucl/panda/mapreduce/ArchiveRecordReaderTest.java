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

import uk.ac.ucl.panda.mapreduce.indexing.PandaInputFormat;

import junit.framework.TestCase;

public class ArchiveRecordReaderTest extends TestCase {

	@Test
	public void testReadTopics() throws IOException, InterruptedException {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");

		// TODO: this should not be hardcoded.
		File testFile = new File("/home/ib/irdm/smalldocs/fb396001.z");
		FileSplit split = new FileSplit(
		       new Path(testFile.toURI()), 0, 
		       (long)testFile.length(), null); 
		PandaInputFormat inputFormat = new PandaInputFormat();
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		RecordReader<Text, Text> reader = inputFormat.createRecordReader(split, context);
		reader.initialize(split, context);
		
		int num_topics = 0;
		while (reader.nextKeyValue()) {
			num_topics += 1;
		}
		assertEquals(49, num_topics);
		assertEquals(new Text("FBIS3-49"), reader.getCurrentKey());
		assertTrue(reader.getCurrentValue().find("Pyongyang Threatens NPT Withdrawal".toUpperCase()) > 0);
	}
}
