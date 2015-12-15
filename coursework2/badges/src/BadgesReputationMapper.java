import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.MRDPUtils;
import java.util.Map;
import java.util.HashMap;

public class BadgesReputationMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Map<String, Integer> userReputation;
	private IntWritable reputation;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());		
		String userId = parsed.get("UserId");
		String badge = parsed.get("Name");
		if (userId != null && userReputation.get(userId) != null
			&& Character.isUpperCase(badge.charAt(0))) {
			reputation = new IntWritable(userReputation.get(userId));	
			context.write(new Text(badge), reputation);
		}	
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		userReputation = new HashMap<String, Integer>();

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
				if (map.get("Reputation") != null) {
					userReputation.put(map.get("Id"), Integer.parseInt(map.get("Reputation")));
				}
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}
