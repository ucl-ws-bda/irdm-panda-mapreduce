package uk.ac.ucl.panda.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import uk.ac.ucl.panda.map.MAPScore;
import uk.ac.ucl.panda.map.ResultsList;
import uk.ac.ucl.panda.map.ResultsList.Result;
import uk.ac.ucl.panda.mapreduce.io.ScoreDocPair;

public class RetrievalEvaluator {
	
	private static Logger logger = Logger.getLogger(RetrievalEvaluator.class);

	public static Map<Long, List<Result>> readPandaResults() throws ClassNotFoundException, IOException {
		ResultsList results = MAPScore.getResultsFromFile();
				
		Comparator<Result> sortByRank = new Comparator<Result>() {
	        @Override
	        public int compare(Result r1, Result r2) {
	            return ((Integer)r1.rank).compareTo(r2.rank);
	        }
	    };
	    
	    Map<Long, List<Result>> resultIndex = new HashMap<Long, List<Result>>();
	    ArrayList<Result> rs;
	    
	    for (Integer topicId: results.getTopics()) {
	    	rs = results.getTopicResults(topicId);
			Collections.sort(rs, sortByRank);
			resultIndex.put(topicId.longValue(), rs.subList(0, Math.min(rs.size(), 100)));
	    }
	    return resultIndex;
	}
	
	public static Map<Long, ArrayWritable> readResults(Path path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(path, false);
		Map<Long, ArrayWritable> resultsIndex = new HashMap<Long, ArrayWritable>();	
		LongWritable key = new LongWritable();
		ArrayWritable results = new ArrayWritable(ScoreDocPair.class);
		Configuration conf = new Configuration();		
		while (fileIter.hasNext()) {
			LocatedFileStatus f = fileIter.next();
			if (f.isFile()) {	
				logger.info("Reading results file " + f.getPath().getName());
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(f.getPath()));
				while (reader.next(key, results)) {
					resultsIndex.put(key.get(), results);
					key = new LongWritable();
					results = new ArrayWritable(ScoreDocPair.class);
				}			
				reader.close();
			}
		}
		return resultsIndex;
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException {
		if (args.length != 1) {
			System.out.println("Usage: retrievalResultsDir.");
			System.exit(-1);
		}
		/**
		PostingWritable posting = Index.fetchPosting("/home/ib/irdm/index_test/out/", Stemmer.stem("Mining"));
		System.out.println("CTF: " + posting.getCollectionTermFrequency());
		System.out.println("DF: " + posting.getDocumentFrequency());
		for (Writable docId: posting.getObservations().keySet()) {
			System.out.println(docId);
		}
		 **/
		Map<Long, ArrayWritable> resultsIndex = readResults(new Path(args[0]));
		Map<Long, List<Result>> pandaResultsIndex = readPandaResults();
		
		
		int topics = 0;
		for (Long topicId : pandaResultsIndex.keySet()) {
			if (!resultsIndex.containsKey(topicId)) continue;
			topics += 1;
			int results = 0;
			Writable[] ours = resultsIndex.get(topicId).get();
			List<Result> theirs = pandaResultsIndex.get(topicId);
			Set<String> ourset = new HashSet<String>();
			Set<String> theirset = new HashSet<String>();
			for (int i = 0; i < ours.length && i < theirs.size(); i++) {
				ourset.add(((ScoreDocPair)ours[i]).getDocId().toString());
				theirset.add(theirs.get(i).docID);
				if (theirs.get(i).docID.equals(((ScoreDocPair)ours[i]).getDocId().toString())) {
					results += 1;
				}
			}
			System.out.print(topicId + " matches on " + results + " out of " + theirs.size());
			theirset.retainAll(ourset);
			System.out.print(" with set intersection " + theirset.size());
			System.out.println(" (" + theirs.size() + ", " + ours.length + ")");
		}
		System.out.println("Results shared " + topics + "/50 queries.");
	}

}
