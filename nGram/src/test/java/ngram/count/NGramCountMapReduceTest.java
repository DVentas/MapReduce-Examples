package ngram.count;

import java.io.IOException;

import ngram.count.NGramCountMapper;
import ngram.count.NGramCountReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class NGramCountMapReduceTest {

	private static final int NUM_NGRAMS = 2;
	
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		this.mapReduceDriver = MapReduceDriver.newMapReduceDriver(new NGramCountMapper(), new NGramCountReducer());
		
		this.mapReduceDriver.getConfiguration().setInt("map.numberOfNGram", NUM_NGRAMS);
		
	}
	
	
	@Test
    public void testMoreThanFiveOcurrencesIsOutput() throws IOException {
		
		this.mapReduceDriver
			
			.withInput(new LongWritable(1),new Text("Ese barco azul"))
			.withInput(new LongWritable(6),new Text("Ese barco amarillo"))
			
			.withOutput(new Text("barco amarillo"), new LongWritable(1))
			.withOutput(new Text("barco azul"), new LongWritable(1))
			.withOutput(new Text("ese barco"), new LongWritable(2));
		
		this.mapReduceDriver.runTest();
	 }
	
}
