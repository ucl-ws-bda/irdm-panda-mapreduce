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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.io.Files;

import uk.ac.ucl.panda.mapreduce.io.Index;
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
	
	@Test
	public void testStoreAndRetrievePosting() throws IOException {
		Index.writeIndex(index, testDirectory);
		Map<Text, PostingWritable> fetchedIndex = Index.readIndex(testDirectory);
		
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
	
	@Test
	public void testStoreAndRetrievePostingOneByOne() throws IOException {
		Index.writeIndex(index, testDirectory);
		
		for (Text key: index.keySet()) {
			PostingWritable a = index.get(key);
			
			PostingWritable b = Index.fetchPosting(testDirectory.getAbsolutePath(), key.toString());
			
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
