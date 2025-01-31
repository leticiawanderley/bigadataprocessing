package bdp.stock;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import bdp.stock.*;

public class DailyMaxMapper extends Mapper<Text, DailyStock, Text, DoubleWritable> { 
    public void map(Text key, DailyStock value, Context context) throws IOException, InterruptedException {
        context.write(value.getCompany(), value.getClose());
        }
}
