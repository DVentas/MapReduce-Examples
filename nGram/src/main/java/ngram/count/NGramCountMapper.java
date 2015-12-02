package ngram.count;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	private final LongWritable one = new LongWritable(1);
	private static final String REGEX_WORDS = "\\P{L}+";
	private static final String SPACE= "\\s+";
	private static final String EMPTY= " ";
	private Text outKey = new Text();
	private String[] wordsCleaned;
	private int wordIndex;
	private int numberOfNGram;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		numberOfNGram = context.getConfiguration().getInt("map.numberOfNGram", 3);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException{

		wordsCleaned = value.toString().replaceAll(REGEX_WORDS, EMPTY).toLowerCase().trim().split(SPACE);
		
		if (!StringUtils.isEmpty(wordsCleaned[0])){
			for (wordIndex = 0; (wordIndex + numberOfNGram) <= wordsCleaned.length; wordIndex++) {
				
				outKey.set(StringUtils.join(Arrays.copyOfRange(wordsCleaned, wordIndex, (wordIndex + numberOfNGram)), EMPTY));
				context.write(outKey, one);
				
			}
		}
	}

}
