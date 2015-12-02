package topkwords;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKWordsCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	
	private String [] wordCount;
	
	private SortedSet<WordCount> sortValues;
	
	private int kMaxValues;
	
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		kMaxValues = context.getConfiguration().getInt("map.numberOfKWords", 10);

		this.sortValues = new TreeSet<WordCount>();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException{
		
		wordCount = value.toString().trim().split("\t");
		
		sortValues.add( new WordCount(new Text(wordCount[0]), new LongWritable(new Long(wordCount[1]))));
		
		if (sortValues.size() > kMaxValues) {
			sortValues.remove(sortValues.last());
		}
	}
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		for (WordCount entries : this.sortValues.tailSet(this.sortValues.first())) {
			context.write(entries.getKey(), entries.getValue());
		}
	}
	
}
