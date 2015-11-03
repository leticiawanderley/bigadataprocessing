import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.sql.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private final int GO_OFFSET = 2;
    private final int TEAM_OFFSET = 4;	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String h = value.toString().split(";")[2];
	String[] hashtags = h.split("\\s+"); 
	for (String hashtag : hashtags) {
		hashtag = hashtag.toLowerCase();
		if (hashtag.startsWith("go")) {
			context.write(new Text(hashtag.substring(GO_OFFSET)), one);		
		}
		else if (hashtag.startsWith("team")) {
			context.write(new Text(hashtag.substring(TEAM_OFFSET)), one);		
		}	
	}		
    }
}
