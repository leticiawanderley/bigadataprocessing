import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;
import utils.IntArrayPair;

public class TagResponseCombiner extends Reducer<Text, IntArrayPair, Text, IntArrayPair> {

    public void reduce(Text key, Iterable<IntArrayPair> values, Context context)
              throws IOException, InterruptedException {
        int sum = 0;
	String[] tags = new String[0]; 
   	for (IntArrayPair value : values) {
	    if (value.getFirst().get() == 1) {
		sum += 1;
	    }
	    else {
	    	tags = value.getSecond().toStrings();
   	    }	
        } 
	IntArrayPair result = new IntArrayPair();   
	result.set(new IntWritable(sum), new ArrayWritable(new String[0]));
	for (String tag : tags) {
		context.write(new Text(tag.replaceAll("\\s+","")), result);
	}	
    }
}
