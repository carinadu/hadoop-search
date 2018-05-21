package code.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.CorpusFetcher;
import util.PostingList;
import util.QueryParser;
import util.QueryParser.Query;
import util.SearchResult;
import util.Stemmer;

public class QueryMapred {
	/**
	 * QueryMapred: Take in a query in command line and search in inverted index
	 * parameters:
	 * 	indexFile: default inverted index folder
	 * 	queryOutput: default result output folder
	 * 	partitionFile: default inverted index partition file
	 */
	private static String indexFile = "inverted";
	private static String queryOutput = "result";
	private static String partitionFile = "_index_partition";
	private static String stopwordsFile = "donttouch/stopwords/part-r-00000";
	
	public static class QueryMapper extends Mapper<Text, Text, NullWritable, Text> {
		/**
		 * QueryMapper
		 * parameters:
		 * 	terms: tokenized query terms
		 * 
		 */
		private Stemmer stmr = new Stemmer();
		private List<String> terms;
		private Text postingList = new Text();
		public void map(Text key,  Text value, Context context) throws IOException, InterruptedException {
			if(terms.contains(key.toString())) {
				postingList.set(key.toString().concat(" ").concat(value.toString()));
				context.write(NullWritable.get(), postingList);
			}
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			/**
			 *  get the query from configuration and put all useful terms into terms
			 *  split by either ( or )
			 *  only add when the term is not in ("not", "and", "or")
			 */
			terms = new ArrayList<String>();
			String query = context.getConfiguration().get("query");
			for(String term : query.split("[ \\(\\)]")) {
				if(term.isEmpty() || term.equals("and") || term.equals("or") || term.equals("not")) continue;
				stmr.add(term.toCharArray(), term.length());
				stmr.stem();
				terms.add(stmr.toString());
			}
		}
	}
	
	public static class QueryReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
		/**
		 * QueryReducer: take (word, postingList) and turn it into search result
		 * parameters:
		 * 	q: a query object
		 * 
		 */
		private Query q;
		private Text result = new Text();
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, PostingList> terms = new HashMap<String, PostingList>();
			for(Text value : values) {
				String[] strs = value.toString().split(" ");
				terms.put(strs[0], new PostingList(strs[1]));
			}
			SearchResult res = new SearchResult();
			PostingList postings = q.evaluate(terms).list;
			postings.sortPostingByScore();
			res.readFromPostingList(postings);
			result.set(res.toString());
			context.write(result, NullWritable.get());
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			/**
			 * get query from configuration and parse the query into a query object
			 */
			QueryParser.STOP_WORD_SET.clear();
			try(FileSystem fs = FileSystem.newInstance(context.getConfiguration()); 
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(stopwordsFile))))) {
				String word;
				Stemmer stmr = new Stemmer();
				while((word = reader.readLine()) != null) {
					stmr.add(word.toCharArray(), word.length());
					stmr.stem();
					word = stmr.toString();
					QueryParser.STOP_WORD_SET.add(word);
				}
			}
			String query = context.getConfiguration().get("query");
			QueryParser parser = new QueryParser();
			this.q = parser.parseQuery(query);
		}

	}
	
	private static List<String> fetchInputFiles(Configuration conf, Path partitionPath, String dir, String query){
		/**
		 *  check each query terms in partition file for inverted index
		 *  only add file which contains the term we want to improve performance
		 *  if there is no partition file, only the first file will be used
		 *  
		 *  Parameter: 
		 *  	conf:
		 *  	partitionPath: the path of partition file for inverted index
		 *  	dir: inverted index directory
		 *  	query:
		 *  Return: a list of filenames to be add 
		 */
		
		// fetch boundaries for lemma
		List<String> boundaries = new ArrayList<String>();
		List<String> filenames = new ArrayList<String>();
		try(SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(partitionPath))) {
			Text word = new Text();
			while(reader.next(word)) boundaries.add(word.toString().split(":")[0]);
		}catch(IOException e) {}
		
		// get corresponding file names
		String head = dir.concat(File.separator).concat("part-r-");
		for(String term : query.split("[ \\(\\)]")) {
			if(term.isEmpty() || term.equals("and") || term.equals("or") || term.equals("not")) continue;
			int i = 0;
			for(String bdry : boundaries)
				if(term.compareTo(bdry) < 0) break;
				else ++i;
			String filename = head.concat(String.format("%05d", i));
			if(!filenames.contains(filename)) filenames.add(filename);
		}
		
		System.out.println(filenames);
		return filenames;
	}
	
	public static String runMapred(Configuration conf, String query, String[] otherArgs) throws Exception{
		conf.set("query",query);
		// Parse arguments
		String indexFile = QueryMapred.indexFile;
		String queryOutput = QueryMapred.queryOutput;
		for(int i = 2; i < otherArgs.length; i += 2) {
			if(!otherArgs[i].startsWith("-") || i + 1 >= otherArgs.length) {
				System.err.println("Usage: query page (-i indexFile) (-o outputFile)");
				System.exit(0);
			}
			if(otherArgs[i].equals("-i")) {
				String dir = otherArgs[i + 1].concat(File.separator);
				indexFile = dir.concat(indexFile);
				partitionFile = dir.concat(partitionFile);
			}else if(otherArgs[i].equals("-o")) queryOutput = otherArgs[i + 1];
		}
		Path outPath = new Path(queryOutput);
		Path partitionPath = new Path(partitionFile);
		
		// Delete output when it already exists
		FileSystem fs = FileSystem.newInstance(conf);
		if(fs.exists(outPath)) fs.delete(outPath, true);
		
		Job job = Job.getInstance(conf, "do query");
		
		job.setJarByClass(QueryMapred.class);
		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Add all files needed to InputPath
		for(String filename : fetchInputFiles(conf, partitionPath, indexFile, query))
			FileInputFormat.addInputPath(job, new Path(filename));
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		return queryOutput;
	}
	
	public static void main(String[] args) throws Exception{
		/**
		 * entrance of query
		 * REQUIRED query in arguments, other arguments are optional
		 * use -i to specify inverted index folder which contains "inverted" directory
		 * use -o to specify query result folder
		 */
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "hadoop02");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Error when no query
		if(otherArgs.length < 1) {
			System.err.println("Usage: query page (-i indexFolder) (-o outputFolder)");
			System.exit(0);
		}
				
		CorpusFetcher fetcher = new CorpusFetcher(conf);
		String query = otherArgs[0].toLowerCase();
		SearchResult sr = fetcher.isInCache(query);
		if(sr == null) {
			String output = runMapred(conf, query, otherArgs);
			Path outPath = new Path(output.concat(File.separator).concat("part-r-00000"));
			try(FileSystem fs = FileSystem.get(conf); 
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outPath)))) {
				sr = new SearchResult();
				sr.readFromString(reader.readLine());
				fetcher.writeQueryToCache(query, sr.toString());
			}
		}
		String pageNum = otherArgs[1];
		fetcher.writeResult(sr, Integer.valueOf(pageNum), query);
	}
}
