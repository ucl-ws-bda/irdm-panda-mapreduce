package uk.ac.ucl.panda.mapreduce.retrieval;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import uk.ac.ucl.panda.indexing.io.TrecTopicsReader;
import uk.ac.ucl.panda.retrieval.query.QualityQuery;

public class TopicRecordReader extends RecordReader<LongWritable, Text> {
	private QualityQuery qqs[];
	private BufferedReader br;
	private TrecTopicsReader qReader;
	private LongWritable key;
	private Text value;
	private int progress = 0;
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration conf = context.getConfiguration();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream toipicsFile = fs.open(split.getPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(toipicsFile));
        TrecTopicsReader qReader = new TrecTopicsReader();	    
        qqs = qReader.readQueries(br);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (progress >= qqs.length) {
        	return false;
        }
		
		key = new LongWritable();
        value = new Text();
        QualityQuery qq = qqs[progress];
        key.set(Long.parseLong(qq.getQueryID()));
        value.set(qq.getValue("title"));
        progress += 1;
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// Progress is the 1-indexed next position we are at in the list of topics.
		return progress + 1;
	}

	@Override
	public void close() throws IOException {
		// Could arguably be closed during initialisation unless we expect
		// to need to re-read the topics file during processing due to errors.
		if (br != null) {
			br.close();
		}
	}

}
