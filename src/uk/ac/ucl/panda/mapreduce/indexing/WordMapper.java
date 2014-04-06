package uk.ac.ucl.panda.mapreduce.indexing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import uk.ac.ucl.panda.mapreduce.io.PairOfStringInt;
import uk.ac.ucl.panda.mapreduce.util.Index;
import uk.ac.ucl.panda.mapreduce.util.ObjectFrequencyDistribution;
import uk.ac.ucl.panda.mapreduce.util.Pair;

public class WordMapper extends Mapper<Text, Text, Text, PairOfStringInt> {
	private final Text WORD = new Text();
	private final ObjectFrequencyDistribution<String> DISTRIBUTION = new ObjectFrequencyDistribution<String>();

	private MultipleOutputs<Text, LongWritable> mos;
	
	public void setup(Context context) {
	 		mos = new MultipleOutputs(context);
	}
	 
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String doc = value.toString();
		StringTokenizer terms = new StringTokenizer(doc);
		DISTRIBUTION.clear();

		// get tf
		while (terms.hasMoreTokens()) {
			DISTRIBUTION.increment(terms.nextToken());
		}

		long numTerms = 0;
		// emit word and posting
		for (Pair<String, Integer> posting : DISTRIBUTION) {
			WORD.set(posting.getLeftElement());
			context.write(
					WORD,
					new PairOfStringInt(key.toString(), posting
							.getRightElement()));
			numTerms += posting.getRightElement();
		}			
		context.getCounter(IndexMeta.CollectionLength).increment(numTerms);
		context.getCounter(IndexMeta.NumberOfDocuments).increment(1);
		// needed in panda model (just fetched when model is used)
		//results in 2000 m files 
		//post processing: collect all and create one file
		//post processing: 
		mos.write(Index.docIndexDir, key, new LongWritable(numTerms));
	}		
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
    }
}