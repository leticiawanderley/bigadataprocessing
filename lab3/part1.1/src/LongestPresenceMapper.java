import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LongestPresenceMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> { 
    private final NullWritable n = NullWritable.get();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
	MapWritable result = new MapWritable();
	result.put(new Text(fields[0]), new IntWritable(Integer.parseInt(fields[1])));
        context.write(n, result);
        }
}
