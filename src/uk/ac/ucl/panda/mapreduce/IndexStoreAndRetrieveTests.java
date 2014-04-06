package uk.ac.ucl.panda.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.io.Files;

import uk.ac.ucl.panda.mapreduce.io.PostingWritable;
import uk.ac.ucl.panda.mapreduce.util.Index;


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
			posting.addObservation(new Text("Random" + randomGenerator.nextLong()),
								   new IntWritable(randomGenerator.nextInt())
			);
		}
		return posting;		
	}
		
	@Test
	public void testStoreAndRetrievePosting() throws IOException {
		Index.writeIndex(index, new Path(testDirectory.getPath()));
		Map<Text, PostingWritable> fetchedIndex = Index.readIndex(new Path(testDirectory.getPath(), Index.indexDir));
		
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
				int atf = ((IntWritable)aObs.get(docId)).get();
				int btf = ((IntWritable)bObs.get(docId)).get();				
				assertEquals(atf, btf);
			}
		}
	}
	
	@Test
	public void testStoreAndRetrievePostingOneByOne() throws IOException {
		Index.writeIndex(index, new Path(testDirectory.getPath()));
		
		for (Text key: index.keySet()) {
			PostingWritable a = index.get(key);
			
			PostingWritable b = Index.fetchPosting(testDirectory.getAbsolutePath(), key.toString());
			
			assertEquals(a.getCollectionTermFrequency(), b.getCollectionTermFrequency());
			assertEquals(a.getDocumentFrequency(), b.getDocumentFrequency());
			
			MapWritable aObs = a.getObservations();
			MapWritable bObs = b.getObservations();
			for (Writable docId: aObs.keySet()) {				
				assertTrue(bObs.containsKey(docId));
				assertEquals(a.getCollectionTermFrequency(), b.getCollectionTermFrequency());
				assertEquals(a.getDocumentFrequency(), b.getDocumentFrequency());
			}
		}
	}

}
