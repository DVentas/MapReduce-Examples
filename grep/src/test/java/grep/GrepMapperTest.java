package grep;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class GrepMapperTest {
	
	private static final String WORD_TO_FIND = "world";
	private static final String TEXT_WITH_COINCIDENCE = "Hello world";
	private static final String TEXT_WITHOUT_COINCIDENCE = "Hola mundo";

	private MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;
	
	@Before
	public void setUp () {
		this.mapDriver = new MapDriver<LongWritable, Text, Text, NullWritable>()
				.withMapper(new GrepMapper());
		
		this.mapDriver.getContext().getConfiguration().set("map.wordToFind", WORD_TO_FIND);
	}
	
	@Test
    public void testCoincidence() throws IOException {
		 this.mapDriver		 
        		 .withInput(new LongWritable(1),new Text(TEXT_WITH_COINCIDENCE))
        		 .withOutput(new Text(TEXT_WITH_COINCIDENCE), NullWritable.get());
         
         this.mapDriver.runTest();
	 }
	
	@Test
    public void testWithoutCoincidence() throws IOException {
         this.mapDriver
        		.withInput(new LongWritable(1),new Text(TEXT_WITHOUT_COINCIDENCE));
         
         this.mapDriver.runTest();
	 }
	
}
