package ngram.count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ngram.count.NGramCountReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class NGramCountReducerTest {

	private static final String TEXT = "ese barco azul";
	
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	
	@Before
	public void setUp() {
		this.reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>()
       		 .withReducer(new NGramCountReducer());
	}
	
	@Test
    public void testAddMatches() throws IOException {
		
		 List<LongWritable> values = new ArrayList<LongWritable>();
		 values.add(new LongWritable(1));
		 values.add(new LongWritable(3));
		 values.add(new LongWritable(5));
		 values.add(new LongWritable(1));
		 
         this.reduceDriver        
        		 .withInput(new Text(TEXT), values)         
        		 .withOutput(new Text(TEXT), new LongWritable(10));
         
         this.reduceDriver.runTest();
	 }
	
}
