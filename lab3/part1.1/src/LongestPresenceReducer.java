import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongestPresenceReducer extends Reducer<NullWritable, MapWritable, Text, IntWritable> {
    private Text name;
    private IntWritable longestPresence = new IntWritable(0);		
    public void reduce(NullWritable n, Iterable<MapWritable> values, Context context)
              throws IOException, InterruptedException {
        for (MapWritable value : values) {
            Text[] y = value.keySet().toArray(new Text[0]);
	    Text stock = y[0];
	    IntWritable v = (IntWritable) value.get(stock);	
	    if (v.get() > longestPresence.get()) {
		longestPresence = v;
		name = stock;
	    }		
        }    
	
	context.write(name, longestPresence);		   
    }
}
