package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.StringBuilder;
import java.lang.Double;
import java.util.Collection;

public class VolumePerYearReducer extends Reducer<TextIntPair, LongWritable, Text, LongWritable> {
    public void reduce(TextIntPair pair, Iterable<LongWritable> values, Context context)
              throws IOException, InterruptedException {
	long sum = 0;	
        StringBuilder builder = new StringBuilder();
        for (LongWritable value : values) {
            sum += value.get(); 
        }    
	builder.append(pair.getLeft().toString())
		.append(",")
		.append(pair.getRight())
		.append(",");
	Text result = new Text(builder.toString());
	context.write(result, new LongWritable(sum));		   
    }
}
