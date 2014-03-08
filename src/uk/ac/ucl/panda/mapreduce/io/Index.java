package uk.ac.ucl.panda.mapreduce.io;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

public class Index {
	
	public static Map<Text, PostingWritable> readIndex(File directory) throws IOException {
		Map<Text, PostingWritable> index = new HashMap<Text, PostingWritable>();		
		
		for (File child : directory.listFiles()) {		
			if (child.getName().startsWith(".") || child.getName().startsWith("_")) {
				continue; // Ignore the self and parent aliases.
			}
			
			System.out.println("Looking at " + child.toString());
			Path p = new Path(child.getAbsolutePath());
			Configuration conf = new Configuration();			
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(p));
			Text key = new Text();
			PostingWritable posting = new PostingWritable();
			while (reader.next(key, posting)) {
				index.put(key, posting);
				key = new Text();
				posting = new PostingWritable();
			}
			reader.close();
		}
		return index;
	}
	
	public static void writeIndex(Map<Text, PostingWritable> index, File outputFile) throws IOException {
		Configuration conf = new Configuration();
		Path p = new Path(outputFile.getAbsolutePath());		
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf, 
				Writer.file(p), 
				Writer.keyClass(Text.class),
	            Writer.valueClass(PostingWritable.class)
	    );
		
		for (Text key: index.keySet()) {
			writer.append(key, index.get(key));
		}
		writer.close();
	}
}
