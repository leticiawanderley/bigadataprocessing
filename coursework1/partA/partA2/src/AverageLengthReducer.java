import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AverageLengthReducer extends Reducer<NullWritable, IntWritable, DoubleWritable, DoubleWritable> {
    private DoubleWritable result140 = new DoubleWritable();
    private DoubleWritable result = new DoubleWritable();
	
    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
	double size140 = 0;
	double size = 0;
	double sum140 = 0;
	double sum = 0;
   	for (IntWritable value : values) {
            sum += value.get();
	    size++;
	    if (value.get() > 0 && value.get() <= 140) {
         	sum140 += value.get();
		size140++;
            }
        }    
	result.set(sum/size);
	result140.set(sum140/size140);
	context.write(result, result140);	
    }
}

