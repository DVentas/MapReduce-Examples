package ngram.count;

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

public class NGramCountDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		if(args.length != 3) {
			System.err.println("NGramCountDriver required params: <input_path> <output_path> numberOfNGrams");
			System.exit(-1);
		}
		
		this.deleteOutputFileIfExist(args[1]);
		
		final Job nGramCountJob = Job.getInstance(getConf());
		nGramCountJob.setJarByClass(NGramCountDriver.class);
		nGramCountJob.setInputFormatClass(TextInputFormat.class);
		nGramCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		nGramCountJob.getConfiguration().setInt("map.numberOfNGram", Integer.parseInt(args[2]));
		
		nGramCountJob.setMapperClass(NGramCountMapper.class);
		nGramCountJob.setReducerClass(NGramCountReducer.class);
		// We may use a combiner
		nGramCountJob.setCombinerClass(NGramCountReducer.class);
		
		nGramCountJob.setMapOutputKeyClass(Text.class);
		nGramCountJob.setMapOutputValueClass(LongWritable.class);
		
		nGramCountJob.setOutputKeyClass(Text.class);
		nGramCountJob.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(nGramCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(nGramCountJob, new Path(args[1]));

		return nGramCountJob.waitForCompletion(true) ? 0 : 1;
	}
	
	private void deleteOutputFileIfExist(final String filePath) throws IOException {
		final Path output = new Path(filePath);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}
	
	public static void main (String [] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new NGramCountDriver(), args);
	}

}
