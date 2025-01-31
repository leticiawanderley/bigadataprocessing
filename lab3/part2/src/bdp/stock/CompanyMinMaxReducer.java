package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.StringBuilder;
import java.lang.Double;
import java.util.Collection;

public class CompanyMinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text name, Iterable<DoubleWritable> values, Context context)
              throws IOException, InterruptedException {
	DoubleWritable min = new DoubleWritable(Double.MAX_VALUE);
    	DoubleWritable max = new DoubleWritable(Double.MIN_VALUE);	
        StringBuilder builder = new StringBuilder();
        for (DoubleWritable value : values) {
            if (value.get() > max.get()) {
		max.set(value.get());
	    }
	    if (value.get() < min.get()) {
       		min.set(value.get());
            }
        }    
	builder.append("MIN: ")
		.append(min.toString())
		.append(" MAX: ")
		.append(max.toString());
	Text result = new Text(builder.toString());
	context.write(name, result);		   
    }
}
