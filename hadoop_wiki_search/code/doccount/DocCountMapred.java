package code.doccount;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class DocCountMapred {
	/**
	 * DocCountMapred: count the number of documents in the corpus
	 *
	 */
	
	public static class DocCountMapper extends Mapper<IntWritable, Text, NullWritable, IntWritable> {
		/**
		 * DocCountMapper: count 1 when see a file
		 */
		
		private final IntWritable one = new IntWritable(1);
		public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: document id
			 * 	value: document content
			 * 
			 * Output: key is none, value is ONE represent one occurrence
			 */
			context.write(NullWritable.get(), one);
		}
	}
	
	public static class DocCountReducer extends Reducer<NullWritable, IntWritable, IntWritable, NullWritable> {
		/**
		 * DocCountReducer: combine all the numbers and return the total document count
		 */
		
		private IntWritable count = new IntWritable();
		public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/**
			 * Input:
			 * 	key: none
			 * 	value: one occurrence
			 * 
			 * Output: total document number
			 */
			int cnt = 0;
			for(IntWritable value : values) cnt += value.get();
			count.set(cnt);
			context.write(count, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		/**
		 * entrance of count document number
		 * REQUIRED arguments in order:
		 * 	input corpus directory, output document number directory
		 */
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "hadoop02");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2)
			System.out.println("Arguments : [input] [output]");
		
		// remove output if it already exists
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(otherArgs[1]);
		if(fs.exists(outPath)) fs.delete(outPath, true);
		fs.close();
		
		Job job = Job.getInstance(conf, "doc count");
		
		job.setJarByClass(DocCountMapred.class);
		job.setMapperClass(DocCountMapper.class);
		job.setReducerClass(DocCountReducer.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);	
	}
}
