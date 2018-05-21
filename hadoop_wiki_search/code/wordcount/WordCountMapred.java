package code.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.PairWritable;

public class WordCountMapred {
	/**
	 * WordCountMapred: count the word frequency in a document and 
	 * find the stop words based on word count
	 * Parameters:
	 * 	NUM_STOP_WORD: number of stop words to retrieve
	 */
	
	private static final int NUM_STOP_WORD = 100;

	public static class WordCountMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
		/**
		 * WordCountMapper:
		 * Parameters:
		 * 	DELIM: delimiter for tokenizer, containing almost all punctuation and white spaces 
		 */
		
		private static final String DELIM = "  \t\r\n,.:;'\"()[]{}/<>!?|-—–#$&=_*+";
		private Text word = new Text();
		private final IntWritable one = new IntWritable(1);
		
		@Override
		public void map(IntWritable key, Text page, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(page.toString(), DELIM, false);
			while(itr.hasMoreTokens()) {
				String str = itr.nextToken();
				if(str.matches("\\d+")) continue;
				word.set(str.toLowerCase());
				context.write(word, one);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * WordCountReducer: 
		 */
		private Text word = new Text();
		private IntWritable count = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int cnt = 0;
			for(IntWritable one : ones) cnt += one.get();
			word.set(key);
			count.set(cnt);
			context.write(word, count);
		}
		
	}
	
	public static class StopWordMapper extends Mapper<Text, IntWritable, NullWritable, PairWritable> {	
		/**
		 * StopWordMapper: Aggregate all word counts with one key to find stop words
		 */
		private PairWritable pair = new PairWritable();
		
		@Override
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			pair.set(value.get(), key.toString());
			context.write(NullWritable.get(), pair);
		}
	}
	
	public static class StopWordReducer extends Reducer<NullWritable, PairWritable, Text, NullWritable> {
		/**
		 * StopWordReducer: use a priority queue to find the stop words
		 */
		
		Text stopWord = new Text();
		@Override
		public void reduce(NullWritable key, Iterable<PairWritable> pairs, Context context) throws IOException, InterruptedException{
			// use TreeSet as priority queue based on the count of words
			TreeSet<PairWritable> heap = new TreeSet<PairWritable>();
			for(PairWritable pair : pairs) {
				if(heap.size() < NUM_STOP_WORD) heap.add(new PairWritable(pair));
				else if(pair.getCount() > heap.first().getCount()) {
					heap.pollFirst();
					heap.add(new PairWritable(pair));
				}
				
			}
			
			// write stop words to result
			while(!heap.isEmpty()) {
				PairWritable pair = heap.pollFirst();
				stopWord.set(pair.getWord());
				context.write(stopWord, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception{
		/**
		 * entrance for seeking stop words
		 * REQUIRED arguments
		 * 	input: input directory
		 * 	output: output directory
		 */
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "hadoop02");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2)
			System.out.println("Arguments : [input] [output]");
		
		Path temp = new Path("temp"), outPath = new Path(otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(temp)) fs.delete(temp, true);
		if(fs.exists(outPath)) fs.delete(outPath, true);
		
		// Start the first MapReduce to calculate word count;
		Job wcJob = Job.getInstance(conf, "word count");
		
		wcJob.setJarByClass(WordCountMapred.class);
		wcJob.setMapperClass(WordCountMapper.class);
		wcJob.setCombinerClass(WordCountReducer.class);
		wcJob.setReducerClass(WordCountReducer.class);
		
		wcJob.setInputFormatClass(SequenceFileInputFormat.class);
		wcJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(wcJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(wcJob, temp);
		wcJob.waitForCompletion(true);
		
		// Start the second MapReduce to find stop words
		Job swJob = Job.getInstance(conf, "stop word");
		
		swJob.setJarByClass(WordCountMapred.class);
		swJob.setMapperClass(StopWordMapper.class);
		swJob.setReducerClass(StopWordReducer.class);
		swJob.setNumReduceTasks(1);
		
		swJob.setInputFormatClass(SequenceFileInputFormat.class);
		swJob.setMapOutputKeyClass(NullWritable.class);
		swJob.setMapOutputValueClass(PairWritable.class);
		swJob.setOutputKeyClass(Text.class);
		swJob.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(swJob, temp);
		FileOutputFormat.setOutputPath(swJob, outPath);
		swJob.waitForCompletion(true);
		
		// Clean up temp file
		fs.delete(temp, true);
		fs.close();
	}

}
