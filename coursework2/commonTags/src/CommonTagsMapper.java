import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;
import java.util.Map;

public class CommonTagsMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
	if ("1".equals(parsed.get("PostTypeId"))) {
		String[] tags = getTags(parsed.get("Tags"));
		for (String tag : tags) {
			context.write(new Text(tag), one);
		}	
	}
    }

    private String[] getTags(String tags){
    	tags = tags.replace("&lt;", "").replace("&gt", "");
	return tags.split(";");
    }	
} 
