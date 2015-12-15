import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagsHistogramCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
    private Text data = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");
	int q = Integer.parseInt(values[1]);
        context.write(new Text(values[0]), new IntWritable(q));
    }
}
