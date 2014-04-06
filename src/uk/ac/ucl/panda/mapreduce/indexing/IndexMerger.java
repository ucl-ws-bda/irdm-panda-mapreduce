package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.util.Index;


public class IndexMerger {

	private static Logger logger = Logger.getLogger(IndexMerger.class);

	/**
	 * Merge a series of document length index sequence files into one mapfile.
	 */
	public static void mergeDocumentIndex(Path indexDir) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(indexDir, false);
		Map<Text, LongWritable> index = new HashMap<Text, LongWritable>();	
		Text key = new Text();
		LongWritable docLength = new LongWritable();
		Configuration conf = new Configuration();		
		while (fileIter.hasNext()) {
			LocatedFileStatus f = fileIter.next();
			if (f.isFile() && f.getPath().getName().startsWith(Index.docIndexDir + "-")) {	
				logger.info("Merging partial doc length index " + f.getPath().getName());
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(f.getPath()));
				while (reader.next(key, docLength)) {
					index.put(key, docLength);
					key = new Text();
					docLength = new LongWritable();
				}			
				reader.close();
			}
		}
		
		MapFile.Writer writer = new MapFile.Writer(conf, new Path(indexDir, Index.docIndexDir), 
				MapFile.Writer.keyClass(Text.class),
				MapFile.Writer.valueClass(LongWritable.class)
	    );
		
		List<Text> sortedKeys = new ArrayList<Text>();
		sortedKeys.addAll(index.keySet());
		Collections.sort(sortedKeys);
		for (Text k: sortedKeys) {
			writer.append(k, index.get(k));
		}
		writer.close();
	}
	
	/**
	 * Merge a series of index map files into one large mapfile.
	 */
	public static void mergeIndex(Path indexDir) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> fileIter = fs.listLocatedStatus(indexDir);
		Map<Text, PostingWritable> index = new HashMap<Text, PostingWritable>();	
		while (fileIter.hasNext()) {
			LocatedFileStatus f = fileIter.next();			
			if (f.isDirectory() && f.getPath().getName().startsWith("part-r-0")) {	
				logger.info("Merging partial index " + f.getPath().getName());
				index.putAll(Index.readIndex(f.getPath()));
			}
		}		
		Index.writeIndex(index, indexDir);
	}
	
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		if (args.length != 1) {
			System.out.println("RTFM.");
		}
		mergeIndex(new Path(args[0]));
		mergeDocumentIndex(new Path(args[0]));
	}

}
