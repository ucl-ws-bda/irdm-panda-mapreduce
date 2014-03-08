package uk.ac.ucl.panda.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.io.Files;

import uk.ac.ucl.panda.mapreduce.io.FrequencyLocationsPair;
import uk.ac.ucl.panda.mapreduce.io.PostingWritable;


public class IndexStoreAndRetrieveTests extends TestCase {

	private static final File testDirectory = Files.createTempDir();
	private static Map<Text, PostingWritable> index;
	
	private static final Random randomGenerator = new Random();
	
	@Override
	public void setUp() {
		index = new HashMap<Text, PostingWritable>();
		for (int i = 0; i < randomGenerator.nextInt(10); i++) {
			index.put(new Text("term" + i), createPosting());
		}
	}
		
	private PostingWritable createPosting() {
		PostingWritable posting = new PostingWritable();
		posting.setCollectionTermFrequency(new LongWritable(randomGenerator.nextLong()));
		posting.setDocumentFrequency(new LongWritable(randomGenerator.nextLong()));
		for (int i = 0; i < randomGenerator.nextInt(10); i++) {
			posting.addObservation(new LongWritable(randomGenerator.nextLong()),
								   new LongWritable(randomGenerator.nextLong()),
				                   createLocations()
			);
		}
		return posting;		
	}
	
	private ArrayWritable createLocations() {
		int num_locs = randomGenerator.nextInt(10);
		LongWritable[] locations = new LongWritable[num_locs];
		for (int i = 0; i < num_locs; i++) {
			locations[i] = new LongWritable(randomGenerator.nextLong());
		}
		return new ArrayWritable(LongWritable.class, locations);
	}

	private Map<Text, PostingWritable> readIndex(File directory) throws IOException {
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
	
	private void writeIndex(Map<Text, PostingWritable> index, File outputFile) throws IOException {
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
	
	@Test
	public void testStoreAndRetrievePosting() throws IOException {
		writeIndex(index, File.createTempFile("index", "test", testDirectory));
		Map<Text, PostingWritable> fetchedIndex = readIndex(testDirectory);
		
		for (Text key: index.keySet()) {
			PostingWritable a = index.get(key);
			assertTrue(fetchedIndex.containsKey(key));
			
			PostingWritable b = fetchedIndex.get(key);
			
			assertEquals(a.getCollectionTermFrequency(), b.getCollectionTermFrequency());
			assertEquals(a.getDocumentFrequency(), b.getDocumentFrequency());
			
			MapWritable aObs = a.getObservations();
			MapWritable bObs = b.getObservations();
			for (Writable docId: aObs.keySet()) {				
				assertTrue(bObs.containsKey(docId));
				
				FrequencyLocationsPair ap = (FrequencyLocationsPair)aObs.get(docId);
				FrequencyLocationsPair bp = (FrequencyLocationsPair)aObs.get(docId);
				
				assertEquals(ap.getTermFrequency(), bp.getTermFrequency());
				
				Set<String> aLocs = new HashSet<String>();
				Collections.addAll(aLocs, ap.getLocations().toStrings());
				Set<String> bLocs = new HashSet<String>();
				Collections.addAll(bLocs, bp.getLocations().toStrings());
				assertTrue(aLocs.containsAll(bLocs));
				assertTrue(bLocs.containsAll(aLocs));
			}
		}
	}

}
