import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.sql.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");
	String date = line[1].split(", ")[0]; 
	Text day = new Text(date.toString());
	context.write(day, one);		
    }
}
