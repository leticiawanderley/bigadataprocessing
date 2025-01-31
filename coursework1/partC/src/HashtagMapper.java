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
	String name;
	for (String hashtag : hashtags) {
		hashtag = hashtag.toLowerCase();
		if (hashtag.startsWith("go")) {
			name = hashtag.substring(GO_OFFSET);
			if (isCountryName(name) && checkBeginningOfString(name)) {
				context.write(new Text(name), one);
			}		
		}
		else if (hashtag.startsWith("team")) {
			name = hashtag.substring(TEAM_OFFSET);
			if (isCountryName(name) && checkBeginningOfString(name)) {
				context.write(new Text(name), one);
			}
		}
		else if (hashtag.length() == 3 && checkBeginningOfString(hashtag)) {
			context.write(new Text(hashtag), one);		
		}	
	}		
    }

    private boolean isCountryName(String country) {
	return country.length() <= 36 && country.length() >= 2;
    }
	
    private boolean checkBeginningOfString(String name) {
	return !name.startsWith("w") && !Character.isDigit(name.charAt(0));
    }	
}

