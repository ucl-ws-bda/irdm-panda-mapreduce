package uk.ac.ucl.panda.mapreduce.indexing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.ucl.panda.mapreduce.io.PostingWritable;

public class Indexer {

	public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
		
	}
	
	public class SumReducer extends Reducer<Text, Text, Text, PostingWritable> {

	}
}
