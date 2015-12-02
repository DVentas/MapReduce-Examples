package topkwords;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCount implements Comparable<WordCount>{

	private Text key;
	private LongWritable value;
	
	public WordCount (Text key, LongWritable value) {
		this.key = key;
		this.value = value;
	}
	
	@Override
	public boolean equals(Object obj) {
		boolean isEquals = false;
		 if (obj instanceof WordCount)
			 isEquals = this.key.equals(((WordCount) obj).getKey());
		 
		 return isEquals;
	}
	
	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public LongWritable getValue() {
		return value;
	}

	public void setValue(LongWritable value) {
		this.value = value;
	}

	public int compareTo(WordCount o) {
		final WordCount wordCountToCompare = (WordCount) o;
		if (wordCountToCompare.getValue().compareTo(this.value) == 0) {
			return this.key.compareTo(((WordCount) o).getKey());
		}
		return wordCountToCompare.getValue().compareTo(this.value);
	}
	
	@Override
	public String toString() {
		return "key:" + this.key + " value: " + this.value;
	}

}
