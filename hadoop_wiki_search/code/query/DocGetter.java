package code.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import util.CorpusFetcher;

public class DocGetter {
	
	public static void main(String[] args) throws Exception{
		/**
		 * entrance to find the document by its docId
		 * REQUIRE arguments:
		 * 	docId: document id
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length < 1) {
			System.err.println("Usage: docId");
			System.exit(0);
		}
		// use a util class to find the corresponding document
		CorpusFetcher fetcher = new CorpusFetcher(conf);
		fetcher.getFile(Integer.parseInt(otherArgs[0]));
	}
}
