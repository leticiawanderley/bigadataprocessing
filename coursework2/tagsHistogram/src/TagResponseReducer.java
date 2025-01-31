import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.IntArrayPair;

public class TagResponseReducer extends Reducer<Text, IntArrayPair, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntArrayPair> values, Context context)
              throws IOException, InterruptedException {
        int sum = 0;
   	for (IntArrayPair value : values) {
	    if (value.getFirst().get() >= 1) {
		sum += value.getFirst().get();
	    }
        }    
	result.set(sum);
	context.write(key, result);	
    }
}

