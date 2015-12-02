package topkwords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class WordsCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	private final LongWritable one = new LongWritable(1);
	private static final String REGEX_WORDS = "\\P{L}+";
	private static final String SPACE= "\\s+";
	private static final String EMPTY= " ";
	private Text outKey = new Text();
	private String[] wordsCleaned;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException{

		wordsCleaned = value.toString().replaceAll(REGEX_WORDS, EMPTY).toLowerCase().trim().split(SPACE);
		
		if (!StringUtils.isEmpty(wordsCleaned[0])){
			for (String word : wordsCleaned) {
				outKey.set(word);
				context.write(outKey, one);
			}
		}
	}
}
