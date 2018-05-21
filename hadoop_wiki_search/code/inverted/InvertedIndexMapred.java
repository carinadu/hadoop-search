package code.inverted;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.InputSampler.RandomSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.PostingList;
import util.PostingList.Posting;
import util.Stemmer;

public class InvertedIndexMapred {
	/**
	 * InvertedIndexMapred: create inverted index from corpus
	 * Parameters:
	 * 	SEP: separator of elements in posting list, default as ":"
	 * 	POS_SEP: separator of positions of a term in a document, default as ","
	 */
	private static final String SEP = ":";
	private static final String POS_SEP = ",";
	public static class TermFreqMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
		/**
		 * TermFreqMapper: take corpus as input and generate key: (term, docId), value: position
		 * Parameters:
		 * 	stopWords: stop words to exclude
		 * 	stmr: Porter stemmer from external library
		 * 	DELIM: delimiter for tokenizer, default as almost all punctuation and white spaces
		 */
		
		private List<String> stopWords = new ArrayList<String>();
		private Stemmer stmr = new Stemmer();
		
		private static final String DELIM = " \t\r\n,.:;'\"()[]{}/<>!?|-—#$&=_*+";
		private Text wordAndDoc = new Text();
		private IntWritable docPos = new IntWritable();
		
		public void map(IntWritable key, Text page, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: document id
			 * 	page: page content
			 * 
			 * Emit((term, docId), position)
			 */
			StringTokenizer itr = new StringTokenizer(page.toString(), DELIM, false);
			String docId = "" + key.get();
			int pos = 0;  // position counter
			while(itr.hasMoreTokens()) {
				String word = itr.nextToken().toLowerCase();
				++pos;
				// ignore all pure digits, stop words and non-alphanumeric words
				if(!word.matches("[a-z0-9]+") || word.matches("\\d+") || stopWords.contains(word)) continue;
				// stem the term for better match
				stmr.add(word.toCharArray(), word.length());
				stmr.stem();
				word = stmr.toString();
				wordAndDoc.set(word.concat(SEP).concat(docId));
				docPos.set(pos);
				context.write(wordAndDoc, docPos);
			}
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			/**
			 * read stop words file and store all the stop words into a list
			 * no stop words if the file cannot be found.
			 */
			Configuration conf = context.getConfiguration();
			String dir = conf.get("stopWordsPath");
			Path path = new Path(dir.concat(File.separator).concat("part-r-00000"));
			
			try(FileSystem fs = FileSystem.newInstance(conf);
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
				String line;
				while((line = reader.readLine()) != null) {
					stopWords.add(line);
				}
			}catch(FileNotFoundException e) {}
		}
	}
	
	public static class TermFreqReducer extends Reducer<Text, IntWritable, Text, Text> {
		/**
		 * TermFreqReducer: combine all the positions together and count the occurrence of a word 
		 * in a document.
		 * Output: key: (term, docId), value: count(SEP)positions
		 */
		
		private Text wordAndDoc = new Text();
		private Text countAndPos = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> positions, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: (term, docId)
			 * 	positions: list of document positions of a term
			 * 
			 * Emit ((term, docId), count(SEP)positions)
			 */
			int count = 0;
			StringBuffer posting = new StringBuffer();
			List<Integer> pos = new ArrayList<Integer>();
			// store the positions in a list and count the occurrence of the word.
			for(IntWritable position : positions) {
				++count;
				pos.add(position.get());
			}
			// sort the positions in ascending order
			Collections.sort(pos);
			// compress the positions list using relative positions
			int prev = 0;
			for(int p : pos) {
				int offset = p - prev;
				posting.append(posting.length() == 0 ? offset : POS_SEP + offset);
				prev = p;
			}
			wordAndDoc.set(key);
			countAndPos.set(String.valueOf(count).concat(SEP).concat(posting.toString()));
			context.write(wordAndDoc, countAndPos);
		}
	}
	
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {
		/**
		 * InvertedIndexMapred: ((term, docId), count(SEP)positions) and turn it into
		 * (docId, term(SEP)count(SEP)positions) for later calculate inverted document frequency
		 */
		
		private Text word = new Text();
		private Text posting = new Text();
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: (term, docId) pair
			 * 	value: count(SEP)positions
			 * 
			 * Output: (docId, term(SEP)count(SEP)positions)
			 */
			String[] wordAndDoc = key.toString().split(SEP);
			word.set(wordAndDoc[0]);
			posting.set(wordAndDoc[1].concat(SEP).concat(value.toString()));
			context.write(word, posting);
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		/**
		 * InvertedIndexReducer: calculate tf-idf score of each term in a document
		 * tf-idf score = (1 + log(term frequency)) * log(docNum/document frequency)
		 * Parameters:
		 * 	docNum: total document number
		 * 
		 * Output: (term, list of (docId, score, position))
		 */
		
		private int docNum;
		private Text word = new Text();
		private PostingList postings = new PostingList();
		private Text val = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: document id
			 * 	values: list of (term, count, positions)
			 * 
			 * Output: (term, list of (docId, score, position))
			 */
			int dcount = 0;
			List<String> listStr = new ArrayList<String>();
			// calculate document frequency
			for(Text value : values) {
				String str = value.toString();
				++dcount;
				listStr.add(str);
			}
			word.set(key);
			// Initialize a posting list and sort the postings by the order of document id
			for(String posting : listStr) {
				String[] strs = posting.split(SEP);
				double score = calcTFIDF(Integer.valueOf(strs[1]), dcount);
				postings.addPosting(new Posting(strs[0], score, strs[2]));
			}
			postings.sortPosting();
			val.set(postings.toString());
			context.write(word, val);
			postings.clear();
		}
		
		private double calcTFIDF(int tf, int df) {
			/**
			 * take tf and df and convert it to tf-idf score by
			 * calculate (1 + log(tf)) * log(docNum/df)
			 */
			return (1 + Math.log10(tf)) * Math.log((double)docNum/df);
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			/**
			 * read total number of documents into memory
			 */
			Configuration conf = context.getConfiguration();
			String dir = conf.get("docNum");
			Path path = new Path(dir.concat(File.separator).concat("part-r-00000"));
			
			
			try(FileSystem fs = FileSystem.newInstance(conf);
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
				this.docNum = Integer.valueOf(reader.readLine());
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		/**
		 * entrance of inverted index creation
		 * REQUIRED arguments in order:
		 * 	stop words directory, document number directory, input folder, output folder, output partition file path
		 */
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "hadoop02");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length < 5)
			System.out.println("Arguments : [stopwords] [docnum] [input] [output] [partition_output]");
		
		// put stop words and document number directories into configuration arguments
		conf.set("stopWordsPath", otherArgs[0]);
		conf.set("docNum", otherArgs[1]);
		
		// remove temp and output directory if they already exist
		Path tfTemp = new Path("tf_temp");
		Path outPath = new Path(otherArgs[3]);
		Path partitionPath = new Path(otherArgs[4]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(tfTemp)) fs.delete(tfTemp, true);
		if(fs.exists(outPath)) fs.delete(outPath, true);
		if(fs.exists(partitionPath)) fs.delete(partitionPath, false);
		
		// Start the first MapReduce job to calculate word count;
		Job tfJob = Job.getInstance(conf, "count term frequncy");
		
		tfJob.setJarByClass(InvertedIndexMapred.class);
		tfJob.setMapperClass(TermFreqMapper.class);
		tfJob.setReducerClass(TermFreqReducer.class);
		tfJob.setNumReduceTasks(3);
		
		tfJob.setInputFormatClass(SequenceFileInputFormat.class);
		tfJob.setMapOutputValueClass(IntWritable.class);
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(tfJob, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(tfJob, tfTemp);
		tfJob.waitForCompletion(true);
		
		// Start the second MapReduce job to create inverted index
		Job iiJob = Job.getInstance(conf, "create inverted index");
		
		iiJob.setJarByClass(InvertedIndexMapred.class);
		iiJob.setMapperClass(InvertedIndexMapper.class);
		iiJob.setReducerClass(InvertedIndexReducer.class);
		iiJob.setNumReduceTasks(10); // we partition our output into 10 files
		
		iiJob.setInputFormatClass(KeyValueTextInputFormat.class);
		iiJob.setMapOutputValueClass(Text.class);
		iiJob.setOutputKeyClass(Text.class);
		iiJob.setOutputValueClass(Text.class);
		
		// use a sample to estimate the distribution of terms and partition the output
		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 200, 30);
		TotalOrderPartitioner.setPartitionFile(iiJob.getConfiguration(), partitionPath);
		iiJob.setPartitionerClass(TotalOrderPartitioner.class);
		
		// compress the inverted index file to improve performance
		iiJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(iiJob, CompressionType.BLOCK);
		SequenceFileOutputFormat.setCompressOutput(iiJob, true);
		FileInputFormat.addInputPath(iiJob, tfTemp);
		FileOutputFormat.setOutputPath(iiJob, outPath);
		InputSampler.writePartitionFile(iiJob, sampler);
		iiJob.waitForCompletion(true);
		
		fs.delete(tfTemp, true);
		fs.close();
		
	}
}
