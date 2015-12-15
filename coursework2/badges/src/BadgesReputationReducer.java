import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BadgesReputationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
        int sum = 0;
	int count = 0;
   	for (IntWritable value : values) {
		sum += value.get();
		count += 1;
        }    
	result.set(sum/count);
	context.write(key, result);	
    }
}

