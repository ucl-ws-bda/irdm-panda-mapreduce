package uk.ac.ucl.panda.mapreduce.util;

import uk.ac.ucl.panda.utility.analyzer.PorterStemmer;

public class Stemmer {
	public static PorterStemmer ps = new PorterStemmer();
	
	public static String stem(String word) {
		return ps.stem(word.toLowerCase());
	}

}
