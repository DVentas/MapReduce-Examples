package grep;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<LongWritable, Text, Text, NullWritable >{
	
	private String wordToFind;
	
	private static final int WORD_NOT_FOUND = -1; 
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		wordToFind = context.getConfiguration().get("map.wordToFind");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException{

		if (value.find(wordToFind) != WORD_NOT_FOUND) {
			context.write(value, NullWritable.get());
		}
	}

}
