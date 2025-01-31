import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Math;

public class AverageLengthMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> { 
    private final NullWritable n = NullWritable.get();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");
	int size = line[3].length();
	context.write(n, new IntWritable(size));
    }
}
