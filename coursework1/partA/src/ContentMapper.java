import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Math;

public class ContentMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");
	int size = line[3].length();
	Text clazz = new Text(String.valueOf(Math.ceil(size/5)));
	/* is it just the size of the tweet itself, or the size of the tweet + the size of its hashtags? */
	context.write(clazz, one);
    }
}
