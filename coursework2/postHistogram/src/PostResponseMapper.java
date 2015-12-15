import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;
import java.util.Map;

public class PostResponseMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
	if ("2".equals(parsed.get("PostTypeId"))) {
		context.write(new Text(parsed.get("ParentId")), one);	
	}
    }
}
