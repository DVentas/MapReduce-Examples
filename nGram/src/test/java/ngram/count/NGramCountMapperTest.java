package ngram.count;

import java.io.IOException;

import ngram.count.NGramCountMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class NGramCountMapperTest {

	private static final int NUM_NGRAMS = 3;
	private static final String CLEAN_STRING = "Ese barco azul celeste";
	private static final String DIRTY_STRING = " .:;+*?_-¨{} \r	\t\n La casa?`+.:;+*?_-¨{} \r	\t\n!\"·$%$ &/() \n\t amarilla";
	private static final String SHORT_STRING = "Ese barco";
	
	
	private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	
	@Before
	public void setUp () {
		this.mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>()
				.withMapper(new NGramCountMapper());
		
		this.mapDriver.getContext().getConfiguration().setInt("map.numberOfNGram", NUM_NGRAMS);
	}
	
	@Test
    public void testCleanString() throws IOException {
		this.mapDriver
         		
				.withInput(new LongWritable(1),new Text(CLEAN_STRING))
        		
         		.withOutput(new Text("ese barco azul"), new LongWritable(1))
        		.withOutput(new Text("barco azul celeste"), new LongWritable(1));
         
         this.mapDriver.runTest();
	 }
	
	@Test
    public void testDirtyString() throws IOException {
         this.mapDriver 
         		.withInput(new LongWritable(1),new Text(DIRTY_STRING))
        		.withOutput(new Text("la casa amarilla"), new LongWritable(1));
         
         this.mapDriver.runTest();
         
         
	 }
	
	@Test
    public void testEmptyString() throws IOException {
         this.mapDriver = MapDriver.newMapDriver(new NGramCountMapper())
        		 .withInput(new LongWritable(1),new Text(" .:;+*?_-¨Ç{} \r	\t\n"));
         
         this.mapDriver.runTest();
	 }
	
	@Test
    public void testShortString() throws IOException {
         this.mapDriver = MapDriver.newMapDriver(new NGramCountMapper())
        		 .withInput(new LongWritable(1),new Text(SHORT_STRING));
         
         this.mapDriver.runTest();
	 }
	
	
}
