package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class CorpusFetcher {
	/**
	 * CopursFetcher: helper class to connect and communicate with corpus and cache file
	 * Parameters:
	 * 	conf: hadoop configuration
	 * 	cacheDir: cache directory containing query result
	 * 	corpusDir: directory containing corpus
	 */
	private Configuration conf;
	public static String cacheDir = "querycache";
	public static String corpusDir = "cmpcorpus_map/part-r-00000";
	
	public CorpusFetcher(Configuration conf) {
		this.conf = conf;
	}
	
	public SearchResult isInCache(String query) throws IOException{
		/**
		 * Check if the query is already in cache
		 * Input:
		 * 	query:
		 * 
		 * Output: whether it's present in cache
		 */
		Path path = new Path(cacheDir);
		// return false if the cache file does not exist
		try(FileSystem fs = FileSystem.newInstance(conf)) {
			if(!fs.exists(path)) return null;
		}
		Text key = new Text();
		Text value = new Text();
		// check if the query is in the cache file
		try(MapFile.Reader reader = new MapFile.Reader(path, conf)) {
			key.set(query);
			reader.get(key, value);
			if(value.toString().isEmpty()) return null;
		}
		SearchResult ret = new SearchResult();
		ret.readFromString(value.toString());
		return ret;
	}
	
	public void writeQueryToCache(String query, String res) throws IOException{
		/**
		 * After the query, use this function to write the query result into cache
		 * Input:
		 * 	query:
		 * 	res: query search result
		 */
		Path path = new Path(cacheDir);
		try(MapFile.Writer writer = new MapFile.Writer(conf, path, 
				MapFile.Writer.keyClass(Text.class), MapFile.Writer.valueClass(Text.class))) {
			Text key = new Text(query);
			Text value = new Text(res);
			writer.append(key, value);
		}
	}
	
	public void writeResult(SearchResult res, int page, String query) {
		/**
		 * Fetch the document content according to query result and page number
		 * and write the result to standard output
		 * Input:
		 * 	res: query search result
		 * 	page: page number
		 */
		
		List<List<Integer>> results = res.getResult();
		
		int resultNum = res.getCount();
		int pageNum = results.size();
		
		List<Integer> docs = new ArrayList<>();
		if (pageNum != 0) docs = page > pageNum? results.get(pageNum - 1) : results.get(page - 1);
		List<String> highlights = getHighlightWords(query);
		
		System.out.println("" + resultNum + "/" + pageNum);
		
		Path path = new Path(corpusDir);
		
		IntWritable key = new IntWritable();
		Text pageTxt = new Text();
		try(MapFile.Reader reader = new MapFile.Reader(path, conf)) {
			
			for (Integer doc:docs) {
				
				key.set(doc);
				reader.get(key, pageTxt);
				String wholePage = pageTxt.toString();
				if (wholePage.isEmpty()) continue;
				
				String title = wholePage.substring(0, wholePage.indexOf("\n"));
				String content = wholePage.substring(wholePage.indexOf("\n"));
				
				String abstrct = getAbstract(content, highlights);
				
				System.out.print(doc + "\n" + title + "\n" + abstrct + "$RST$");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public String getAbstract(String content, List<String> highlights) {
		String wrapperStart = "<span style=\"background-color: #FFFF00\">";
		String wrapperEnd = "</span>";
		final int RANGE = 300;
		String result = "";

				
		List<Integer> pos = new ArrayList<Integer>();

		List<int[]> intervals = new ArrayList<int[]>();
		
		String lowerContent = content.toLowerCase();
		
		for (String highlight : highlights) {
			int index = ordinalIndexOf(lowerContent, highlight, 3);
			if (index!=-1){
				pos.add(index);
			}
		}
		
		if (pos.size()==0) {
			if (content.length() < 300) {
				return content.replaceAll("\n", "");
			}
			else {
				return content.substring(0, 300).replaceAll("\n", "") + "...";
			}
		}
		
		int wordRange = RANGE / pos.size();
		
		Collections.sort(pos);
		
		int i = 0;
		
		while (i < pos.size()) {
			int curStart = pos.get(i) - wordRange/2;
			int curEnd = pos.get(i) + wordRange/2;
			while (i+1 < pos.size() && pos.get(i+1)- wordRange/2 < curEnd){
				curEnd = pos.get(i+1)+ wordRange/2;
				i++;
			}
			intervals.add(new int[]{curStart, curEnd});
			i++;
		}
		
		StringBuilder sb = new StringBuilder();
		for (int[] interval : intervals) {
			int s = interval[0];
			int e = interval[1];
			if (s < 0) {
				e += 0 - s;
				s = 0;
			}
			if (e > content.length()-1) {
				s -= e - content.length() - 1;
				if (s < 0) s = 0;
				e = content.length()-1;
			}
			if (s != 0) sb.append("...");
			sb.append(content.substring(s, e+1));
			if (e != content.length()-1) sb.append("...");
		}
		
		result = sb.toString();
		
		for (String highlight : highlights) {
			result = result.replaceAll("\\b" + highlight + "\\b", wrapperStart + highlight + wrapperEnd);
			String caphighlight = highlight.substring(0,1).toUpperCase() + highlight.substring(1);
			result = result.replaceAll("\\b" + caphighlight + "\\b", wrapperStart + caphighlight + wrapperEnd);
		}
		result = result.replaceAll("\n", "");
		
		return result;
	}
	
	public List<String> getHighlightWords(String query) {
		List<String> toHighlight = new ArrayList<String>();
		
		String queryL = query.toLowerCase();
		String[] orClauses = queryL.split("and");
		for (String orClause : orClauses) {
			orClause = orClause.trim();
			boolean neg = false;
			if (orClause.startsWith("not ")) {
				neg = true;
				orClause = orClause.substring(3).trim();
			}
			if (orClause.startsWith("(") && orClause.endsWith(")"))
				orClause = orClause.substring(1, orClause.length()-1).trim();
			String[] literals = orClause.split("or");
			for (String literal : literals) {
				literal = literal.trim();
				if (literal.startsWith("not ")){
					if (!neg) continue;
					literal = literal.substring(3).trim();
					for (String word : literal.split(" "))
						toHighlight.add(word);
				}
				else {
					if (!neg) {
						for (String word : literal.split(" "))
							toHighlight.add(word);
					}
				}
			}
		}
		return toHighlight;
	}
	
	public static int ordinalIndexOf(String str, String substr, int n) {
		int last = -1;
	    int pos = str.indexOf(substr);
	    while (--n > 0 && pos != -1) {
	    	last = pos;
	    	pos = str.indexOf(substr, pos + 1);
	    }
	    return pos == -1? last : pos;
	}
	
	
	/**
	 * This method is used for fetching the file for an input document id.
	 * @param fileId the document id that need to be search
	 * @throws Exception
	 */
	public void getFile(int fileId) throws IOException{
		Configuration conf = new Configuration();
		Path path = new Path("corpusHTML/part-r-00000");
		MapFile.Reader reader = new MapFile.Reader(path, conf);
		IntWritable docId = new IntWritable();
		docId.set(fileId);
		Text posting = new Text();
		reader.get(docId, posting);
		//fix the ref coding problem
		String output = posting.toString().replaceAll("&#60;", "<").replaceAll("&#62;", ">");
		System.out.println(output);
		reader.close();
	}
	
	
	public static void main(String[] args) throws Exception{
		
	}
}
