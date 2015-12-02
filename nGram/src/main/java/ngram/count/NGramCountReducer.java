package ngram.count;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	
	private LongWritable outValue = new LongWritable();
	private long count;
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		
		outValue.set(count);
		context.write(key, outValue);			
	}

}
