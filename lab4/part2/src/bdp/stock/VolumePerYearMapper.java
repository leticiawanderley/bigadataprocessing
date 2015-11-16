package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import bdp.stock.*;

public class VolumePerYearMapper extends Mapper<TextIntPair, LongWritable, TextIntPair, LongWritable> { 
    public void map(TextIntPair key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
        }
}
