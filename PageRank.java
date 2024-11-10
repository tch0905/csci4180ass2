import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PageRank {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Your map logic here
        }
    }

    public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            // Your reduce logic here
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("alpha", args[0]);
        int ite = Integer.parseInt(args[1]);
//        conf.set("iteration", args[3]);
        conf.set("threshold", args[2]);
        String inPath = args[3];
        String outPath = args[4];


        // Set up the Hadoop job
        Job job = Job.getInstance(conf, "page rank");
        Job preJob = PRPreProcess.preConfig(conf,inPath, outPath);
        
        // I think we need to have a for / while loop here to run the job multiple iterations
        boolean success = preJob.waitForCompletion(true);
        preJob.waitForCompletion(true);
        job.setJarByClass(PageRank.class);
//        job.setMapperClass(PRMapper.class);
//        job.setReducerClass(PRReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(PRNodeWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(PRNodeWritable.class);

        // Pass the adjacency list as a configuration to the mapper
//        job.getConfiguration().set("adjacencyList", adjacencyList.toString());
//:

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}