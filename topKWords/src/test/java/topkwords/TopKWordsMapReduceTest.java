package topkwords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TopKWordsMapReduceTest {

	private static final int K_WORDS = 2;
	
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		this.mapReduceDriver = MapReduceDriver.newMapReduceDriver(new TopKWordsCountMapper(), new TopKWordsCountReduce());
		
	}
	
	
	@Test
    public void test2KWords() throws IOException {
		
		this.mapReduceDriver.getConfiguration().setInt("map.numberOfKWords", K_WORDS);
		
		this.mapReduceDriver
			.withInput(new LongWritable(1), new Text("HELLO	8"))
			.withInput(new LongWritable(1), new Text("close	10"))
			.withInput(new LongWritable(1), new Text("open	11"))
			.withInput(new LongWritable(1), new Text("house	2")) 
			.withInput(new LongWritable(1), new Text("sea	6"))
			.withInput(new LongWritable(1), new Text("boat	3")) 	
			
			.withOutput(new Text("open"), new LongWritable(11))
			.withOutput(new Text("close"), new LongWritable(10));
		
		this.mapReduceDriver.runTest();
	 }
	
}
