import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.IntArrayPair;

public class TagResponseAnalysis {

  public static void runJob(String[] input, String output) throws Exception {

    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(TagResponseAnalysis.class);
    job.setMapperClass(TagResponseMapper.class);
    job.setCombinerClass(TagResponseCombiner.class);
    job.setReducerClass(TagResponseReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayPair.class);
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}

 
