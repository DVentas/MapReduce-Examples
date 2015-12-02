package topkwords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordsCountMapReduceTest {
	
	private static final String DIRTY_STRING = " .:;+*?_-¨{} \r	\t\n house?`+.:;+*?_-¨{} \r	\t\n!\"·$%$ &/() \n\t ";

	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		this.mapReduceDriver = MapReduceDriver.newMapReduceDriver(new WordsCountMapper(), new WordsCountReduce());
		
	}
	
	
	@Test
    public void textCleanAndCountWords() throws IOException {
		
		this.mapReduceDriver
			.withInput(new LongWritable(1), new Text("hello world"))
			.withInput(new LongWritable(2), new Text("HELLO WORLD"))
			.withInput(new LongWritable(3), new Text("HOLA HELLO hola hello	"))
			.withInput(new LongWritable(4), new Text("world"))
			
			.withOutput(new Text("hello"), new LongWritable(4))
			.withOutput(new Text("hola"), new LongWritable(2))
			.withOutput(new Text("world"), new LongWritable(3));
		
		this.mapReduceDriver.runTest();
	 }
	
	@Test
    public void testDirtyWord() throws IOException {
		
		this.mapReduceDriver
			.withInput(new LongWritable(1), new Text(DIRTY_STRING))
			
			.withOutput(new Text("house"), new LongWritable(1));
		
		this.mapReduceDriver.runTest();
	 }
	
}
