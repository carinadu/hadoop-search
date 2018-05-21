package code.corpus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;


public class CorpusSplitMapred {
	/**
	 * CorpusSplitMapred: turn Wikipedia xml file into html/string format
	 */
	
	public static class CorpusSplitMapper extends Mapper<LongWritable, WikipediaPage, IntWritable, Text>{
		/**
		 * CorpusSplitMapper: turn Wikipedia xml file into (id, html/string) pairs
		 * Parameters:
		 * 	ctype: the type of output format, 0 = cleaned raw string format, 1 = html displayable format
		 */
		
		private IntWritable id = new IntWritable();
		private Text content = new Text();
		private int ctype = 0;
		public void map(LongWritable key, WikipediaPage page, Context context) throws IOException, InterruptedException {
			try{
				id.set(Integer.parseInt(page.getDocid()));
				content.set(ctype == 0 ? page.getContent() : page.getDisplayContent());
			}catch(NullPointerException e) {
				return;
			}
			context.write(id, content);
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			if(context.getConfiguration().get("corpus_type").equals("html")) ctype = 1;
		}
	}
	
	public static class CorpusSplitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		/**
		 * CorpusSplitReducer:
		 */
		
		private IntWritable id = new IntWritable();
		private Text content = new Text();
		public void reduce(IntWritable key, Text value, Context context)  throws IOException, InterruptedException {
			id.set(key.get());
			content.set(value);
			context.write(id, content);
		}
	}

	public static void main(String[] args) throws Exception{
		/**
		 * entrance of corpus creation
		 * REQUIRED arguments:
		 * 	input directory, output directory
		 * OPTIONAL arguments:
		 * 	corpus_type: html represent html output, string represent cleaned string output
		 */
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "hadoop02");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2) {
			System.err.println("Arguments [input] [output] (html/string)");
			System.exit(0);
		}
		
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(otherArgs[1]);
		
		if(otherArgs.length == 3 && otherArgs[2].equals("html")) conf.set("corpus_type", "html");
		else conf.set("corpus_type", "string");
		
		if(fs.exists(outPath)) fs.delete(outPath, true);
		
		Job job = Job.getInstance(conf, "corpus map");
		
		job.setJarByClass(CorpusSplitMapred.class);
		job.setMapperClass(CorpusSplitMapper.class);
		job.setReducerClass(CorpusSplitReducer.class);
		
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setCompressOutput(job, true);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		
		fs.close();
	}

}
