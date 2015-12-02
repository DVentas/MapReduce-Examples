package grep;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class GrepDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		if(args.length != 3) {
			System.err.println("GrepDriver required params: <input_path> <output_path> wordToFind");
			System.exit(-1);
		}
		
		this.deleteOutputFileIfExist(args[1]);
		
		final Job job = Job.getInstance(getConf());
		job.setJarByClass(GrepDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		
		job.getConfiguration().set("map.wordToFind", args[2]);
		
		job.setMapperClass(GrepMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private void deleteOutputFileIfExist(final String filePath) throws IOException {
		final Path output = new Path(filePath);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}
	
	public static void main (String [] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new GrepDriver(), args);
	}

}
