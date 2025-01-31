import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;
import java.util.Map;

public class DuplicatedTagsMapper extends Mapper<Object, Text, Text, IntWritable> {

	private ArrayList<String> duplicatedPosts;
	private final IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());		
		if ("1".equals(parsed.get("PostTypeId")) && duplicatedPosts.contains(parsed.get("Id"))) {
			String[] tags = getTags(parsed.get("Tags"));
			for (String tag : tags) {
				context.write(new Text(tag), one);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		final String DUPLICATED = "3";

		duplicatedPosts = new ArrayList<String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				Map<String, String> map = MRDPUtils.transformXmlToMap(line);
				if (DUPLICATED.equals(map.get("LinkTypeId"))) {
					duplicatedPosts.add(map.get("RelatedPostId"));
				}				
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

	private String[] getTags(String tags){
    		tags = tags.replace("&lt;", "").replace("&gt", "");
		return tags.split(";");
    	}

}
