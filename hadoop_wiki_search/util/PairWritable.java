package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable, Comparable<PairWritable>{
	private int count;
	private String word;
	
	public PairWritable() {
		
	}
	
	public PairWritable(int count, String word) {
		this.count = count;
		this.word = word;
	}
	
	public PairWritable(PairWritable other) {
		this.count = other.count;
		this.word = other.word;
	}
	
	public long getCount() {
		return count;
	}
	
	public String getWord() {
		return word;
	}
	
	public void set(int count, String word) {
		this.count = count;
		this.word = word;
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		word = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeUTF(word);
	}

	public int compareTo(PairWritable other) {
		return count == other.count ? word.compareTo(other.word) : count - other.count;
	}
}
