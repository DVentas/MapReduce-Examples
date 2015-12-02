package topkwords;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKWordsCountReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	
	private long totalValue;
	
	private int kMaxValues;
	
	private SortedSet<WordCount> sortValues;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		kMaxValues = context.getConfiguration().getInt("map.numberOfKWords", 10);

		this.sortValues = new TreeSet<WordCount>();
	}
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		totalValue = 0;
		for (LongWritable value : values)
			totalValue += value.get();
		
		sortValues.add( new WordCount( new Text(key), new LongWritable(totalValue)));
		
		if (sortValues.size() > kMaxValues) {
			sortValues.remove(sortValues.last());
		}
	}
	
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		for (WordCount entries : this.sortValues.tailSet(this.sortValues.first())) {
			context.write(entries.getKey(), entries.getValue());
		}
		
	}

}
