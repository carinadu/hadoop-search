package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PostingList implements Writable {
	/**
	 * PostingList: posting list of inverted index
	 * Parameters:
	 * 	postings: a list of posting
	 * 	listSep: separator between two postings
	 */
	
	private List<Posting> postings;
	
	public static String listSep = "|";
	
	public static class Posting implements Writable, Comparable<Posting> {
		/**
		 * Posting:
		 * Parameters:
		 * 	docId: document id
		 * 	score: score of a term in a document
		 * 	positions: word positions in the document
		 * 	sep: separator between above parameters
		 * 	posSep: separator between two positions
		 */
		private String docId;
		private double score;
		private List<Integer> positions;
		
		public static String sep = ":";
		public static String posSep = ",";
		
		public Posting() {
			this.positions = new ArrayList<Integer>();
		}
		
		public Posting(String docId, double score, String positions) {
			this.docId = docId;
			this.score = score;
			this.positions = new ArrayList<Integer>();
			for(String pos : positions.split(posSep)) this.positions.add(Integer.valueOf(pos));
		}
		
		public Posting(String docId, double score) {
			this.docId = docId;
			this.score = score;
			this.positions = new ArrayList<Integer>();
		}
		
		public Posting(Posting other) {
			this.docId = other.docId;
			this.score = other.score;
			this.positions = new ArrayList<Integer>();
			for(Integer pos : other.positions) positions.add(pos);
		}
		
		public Posting(String indexStr) {
			this.positions = new ArrayList<Integer>();
			readFromString(indexStr);
		}
		
		public String getDocId() {
			return docId;
		}
		
		public double getScore() {
			return score;
		}
		
		public List<Integer> getPositions() {
			return positions;
		}
		
		
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(docId);
			sb.append(sep);
			sb.append(String.format("%.03f", score));
			sb.append(sep);
			StringBuffer ps = new StringBuffer();
			for(Integer pos : positions) {
				if(ps.length() != 0) ps.append(posSep);
				ps.append(pos);
			}
			sb.append(ps);
			return sb.toString();
		}
		
		public void readFromString(String indexStr) {
			/**
			 * readFromString: reconstruct the object from the string get from toString() method
			 * Input:
			 * 	indexStr: string get from toString() method
			 */
			String[] indices = indexStr.split(sep);
			this.docId = indices[0];
			this.score = Double.parseDouble(indices[1]);
			// deal with situation there is no positions exist
			if(indices.length < 3) return;
			for(String pos : indices[2].split(posSep)) positions.add(Integer.valueOf(pos));
		}
		
		public void readFields(DataInput in) throws IOException {
			String indexStr = in.readUTF();
			readFromString(indexStr);
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(toString());
		}

		@Override
		public int compareTo(Posting other) {
			return docId.compareTo(other.docId);
		}
	}
	
	public PostingList() {
		this.postings = new ArrayList<Posting>();
	}
	
	public PostingList(List<Posting> postings) {
		this.postings = postings;
	}
	
	public PostingList(PostingList other) {
		this.postings = new ArrayList<Posting>();
		for(Posting posting : other.postings) postings.add(posting);
	}
	
	public PostingList(String listStr) {
		this.postings = new ArrayList<Posting>();
		readFromString(listStr);
	}
	
	public void set(PostingList other) {
		postings.clear();
		for(Posting posting : other.postings) postings.add(posting);
	}
	
	public List<Posting> getPosting() {
		return postings;
	}
	
	public void sortPosting() {
		Collections.sort(postings);
	}
	
	public void sortPostingByScore() {
		Collections.sort(postings, new Comparator<Posting>(){
			public int compare(Posting p1, Posting p2) {
				return p1.score == p2.score ? 0 : p1.score < p2.score ? 1 : -1;
			}
		});
	}
	
	public int size() {
		return postings.size();
	}
	
	public Posting get(int index) {
		return postings.get(index);
	}
	
	public void addPosting(Posting p) {
		this.postings.add(p);
	}
	
	public void clear() {
		this.postings.clear();
	}
	
	public void negation() {
		for(Posting p : postings) p.docId = "-".concat(p.docId);
	}
	
	public void readFromString(String listStr) {
		postings.clear();
		if(listStr.isEmpty()) return;
		String listSep = "\\".concat(PostingList.listSep);
		for(String indexStr : listStr.split(listSep))
			postings.add(new Posting(indexStr));
	}

	public void readFields(DataInput in) throws IOException {
		String listStr = WritableUtils.readCompressedString(in);
		readFromString(listStr);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Posting posting : postings) {
			if(sb.length() != 0) sb.append(listSep);
			sb.append(posting);
		}
		return sb.toString();
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeCompressedString(out, this.toString());
	}
	
}
