package uk.ac.ucl.panda.mapreduce.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import uk.ac.ucl.panda.mapreduce.io.PostingWritable;

public class Index {
	
	public static final String metaFile = "metaIndex"; 
	public static final String docIndexDir = "docIndex";
	public static final String indexDir = "index";
	
	public static Map<Text, PostingWritable> readIndex(Path path) throws IOException {
		Map<Text, PostingWritable> index = new HashMap<Text, PostingWritable>();	
		Configuration conf = new Configuration();			
		MapFile.Reader reader = new MapFile.Reader(path, conf);
		Text key = new Text();
		PostingWritable posting = new PostingWritable();
		while (reader.next(key, posting)) {
			index.put(key, posting);
			key = new Text();
			posting = new PostingWritable();
		}			
		reader.close();
		return index;
	}
	
	public static void writeIndex(Map<Text, PostingWritable> index, Path path) throws IOException {
		Configuration conf = new Configuration();
		Path p = new Path(path, indexDir);
		MapFile.Writer writer = new MapFile.Writer(conf, p, 
				MapFile.Writer.keyClass(Text.class),
				MapFile.Writer.valueClass(PostingWritable.class)
	    );
		
		// MapFile requires insertion in sorted order, this will be done 
		// automatically by the second-value sort in MapReduce but necessary here
		// for tests.
		List<Text> sortedKeys = new ArrayList<Text>();
		sortedKeys.addAll(index.keySet());
		Collections.sort(sortedKeys);
		for (Text key: sortedKeys) {
			writer.append(key, index.get(key));
		}
		writer.close();
	}
	
	public static PostingWritable fetchPosting(String directory, String term) throws IOException {
		Path p = new Path(directory, indexDir);
		Configuration conf = new Configuration();			
		MapFile.Reader reader = new MapFile.Reader(p, conf);
		Text key = new Text(term);
		PostingWritable posting = new PostingWritable();
		reader.get(key, posting);
		reader.close();
		return posting;
	}
	
	public static LongWritable fetchDocumentLength(String directory, Text docId) throws IOException {
		Path p = new Path(directory, docIndexDir);
		Configuration conf = new Configuration();			
		MapFile.Reader reader = new MapFile.Reader(p, conf);
		LongWritable dl = new LongWritable();
		reader.get(docId, dl);
		reader.close();
		return dl;
	}
	
	public static Map<String, Long> readMetaIndex(Path path) throws IOException {
		// TODO: very similar to readIndex, consider refactor?
		Map<String, Long> metaIndex = new HashMap<String, Long>();
		Path p = new Path(path, metaFile);
		Configuration conf = new Configuration();			
		MapFile.Reader reader = new MapFile.Reader(p, conf);
		Text key = new Text("unset");
		LongWritable value = new LongWritable(0);
		while(reader.next(key, value)) {
			metaIndex.put(key.toString(), value.get());
		}
		reader.close();
		return metaIndex;
	}
	
	public static void writeMetaIndex(long collectionLength, long numberOfDocuments, Path out) throws IOException {
		Configuration conf = new Configuration();
		Path p = new Path(out, metaFile);
		MapFile.Writer writer = new MapFile.Writer(conf, p, 
				MapFile.Writer.keyClass(Text.class),
				MapFile.Writer.valueClass(LongWritable.class)
	    );
		
		writer.append(new Text("CollectionLength"), new LongWritable(collectionLength));
		writer.append(new Text("NumberOfDocuments"), new LongWritable(numberOfDocuments));
		writer.close();
	}
}
