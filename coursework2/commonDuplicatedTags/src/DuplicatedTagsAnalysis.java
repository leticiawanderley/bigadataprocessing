import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class DuplicatedTagsAnalysis {
    public static void runJob(String[] input, String output) throws Exception {
         
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();
        
        job.setJarByClass(DuplicatedTagsAnalysis.class);
        job.setMapperClass(DuplicatedTagsMapper.class);
	job.setReducerClass(DuplicatedTagsReducer.class);
         
        job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
        
        job.addCacheFile(new Path("/data/stackoverflow/PostLinks").toUri());
        
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
        }
    
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
