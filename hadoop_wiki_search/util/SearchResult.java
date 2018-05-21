package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import util.PostingList.Posting;

public class SearchResult implements Writable{
	/**
	 * SearchResult: result of a query
	 * Parameters:
	 * 	count: number of result documents
	 * 	pages: list of pages, each page contains at most 10 documents
	 * 	innerSep: separator for documents inside a page
	 * 	outerSep: separator for pages
	 */
	private int count;
	private List<List<Integer>> pages;
	public static String innerSep = ",";
	public static String outerSep = ";";
	
	public SearchResult() {
		count = 0;
		this.pages = new ArrayList<List<Integer>>();
	}
	
	public void readFromPostingList(PostingList postings) {
		/**
		 * Use posting list to construct query SearchResult
		 * Input:
		 * 	postings: a posting list returned from query
		 */
		pages.clear();
		List<Posting> list = postings.getPosting();
		for(int i = 0; i < list.size(); ++i) {
			if(i % 10 == 0) pages.add(new ArrayList<Integer>());
			pages.get(pages.size() - 1).add(Integer.valueOf(list.get(i).getDocId()));
		}
		count = list.size();
	}
	
	public void readFromString(String str) {
		/**
		 * convert the string output from toString() back to SearchResult Object
		 */
		pages.clear();
		if(str == null) {
			count = 0;
			return;
		}
		String[] pages = str.split(outerSep);
		count = Integer.valueOf(pages[0]);
		for(int i = 1; i < pages.length; ++i) {
			String page = pages[i];
			List<Integer> ids = new ArrayList<Integer>();
			for(String id : page.split(innerSep))
				ids.add(Integer.valueOf(id));
			this.pages.add(ids);
		}
	}
	
	public int getCount() {
		/**
		 * Get number of result document
		 */
		return count;
	}
	
	public List<List<Integer>> getResult() {
		/**
		 * Get all pages
		 */
		return pages;
	}
	
	public List<Integer> getResult(int page) {
		/**
		 * Return the page required
		 * Input:
		 * 	page : the page number 
		 * 
		 * Output: a page of documents id
		 */
		return pages.get(page - 1);
	}
	
	public String toString() {
		/**
		 * Serialize the object to String
		 */
		StringBuffer sb = new StringBuffer();
		sb.append(count);
		for(int i = 0; i < pages.size(); ++i) {
			List<Integer> page = pages.get(i);
			StringBuffer inner = new StringBuffer();
			for(int j = 0; j < page.size(); ++j) {
				if(inner.length() != 0) inner.append(innerSep);
				inner.append(page.get(j));
			}
			sb.append(outerSep);
			sb.append(inner);
		}
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeCompressedString(out, this.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String listStr = WritableUtils.readCompressedString(in);
		readFromString(listStr);
	}
	
	public static void main(String[] args) {
		SearchResult res = new SearchResult();
		res.readFromString("12;1,2,3,4,5,6,7;4,23,34,23,1");
		System.out.println(res);
	}
}
