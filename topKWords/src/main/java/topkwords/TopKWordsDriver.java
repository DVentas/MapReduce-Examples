package topkwords;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class TopKWordsDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		if(args.length != 3) {
			System.err.println("TopKWords required params: <input_path> <output_path> <k-words>");
			System.exit(-1);
		}
		
		this.deleteOutputFileIfExist(args[1]);
		this.deleteOutputFileIfExist("/tmp/topkwords");
		
		final Job wordCountJob = Job.getInstance(getConf());
		wordCountJob.setJarByClass(TopKWordsDriver.class);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		wordCountJob.setMapperClass(WordsCountMapper.class);
		wordCountJob.setReducerClass(WordsCountReduce.class);
		// We may use a combiner
		wordCountJob.setCombinerClass(WordsCountReduce.class);
		
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(LongWritable.class);
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob,new Path("/tmp/topkwords"));

		if (wordCountJob.waitForCompletion(true)) {
			final Job topKWordsJob = Job.getInstance(getConf());
			
			topKWordsJob.setJarByClass(TopKWordsDriver.class);
			topKWordsJob.setInputFormatClass(TextInputFormat.class);
			topKWordsJob.setOutputFormatClass(TextOutputFormat.class);
			
			topKWordsJob.getConfiguration().setInt("map.numberOfKWords", Integer.parseInt(args[2]));
			
			topKWordsJob.setMapperClass(TopKWordsCountMapper.class);
			topKWordsJob.setReducerClass(TopKWordsCountReduce.class);
			
			topKWordsJob.setMapOutputKeyClass(Text.class);
			topKWordsJob.setMapOutputValueClass(LongWritable.class);
			
			topKWordsJob.setOutputKeyClass(Text.class);
			topKWordsJob.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(topKWordsJob, new Path("/tmp/topkwords"));
			FileOutputFormat.setOutputPath(topKWordsJob, new Path(args[1]));

			return topKWordsJob.waitForCompletion(true) ? 0 : 1;
		} else {
			return 1;
		}
	}
	
	private void deleteOutputFileIfExist(final String filePath) throws IOException {
		final Path output = new Path(filePath);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}
	
	public static void main (String [] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new TopKWordsDriver(), args);
	}

}
