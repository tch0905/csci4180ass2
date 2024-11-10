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
import java.util.StringTokenizer;

public class PageRank {

    public static class ThresholdProcessMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());


            if(itr.hasMoreTokens()){
                String node = itr.nextToken();
                itr.nextToken();
                String p = itr.nextToken();
                if(Boolean.parseBoolean(itr.nextToken())){
                    context.write(new Text(node), new Text(p));
                }
            }
        }
    }

    public static class ThresholdReducers extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            double threshold = Double.parseDouble(conf.get("threshold"));
            if(itr.hasMoreTokens()){
                String node = itr.nextToken();
                String p = itr.nextToken();
                if(Double.parseDouble(p) > threshold){
                    context.write(new Text(node), new Text(p));
                }
            }

        }
    }

    public static Job prAdjustConfig(Configuration conf, String inPath, String outPath, int i)throws Exception{

        Job job = Job.getInstance(conf, "prAdjust "+ i);
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.PRPreMapper.class);
        job.setReducerClass(PRPreProcess.PRPreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("alpha", args[0]);
        int ite = Integer.parseInt(args[1]);
        int counter = 0;
//        float threshold =  Float.parseFloat(args[2]);
        conf.set("threshold", args[2]);
        String inPath = args[3];
        String tempPath = "/temp/";
        String outPath = args[4];


        String tempInputPath = "";
        String tempOutputPath = tempPath + counter;


        // Set up the Hadoop job

        Job preJob = PRPreProcess.preConfig(conf,inPath, tempOutputPath);
        preJob.waitForCompletion(true);


        for (counter = 1; counter < ite; counter++) {

            tempInputPath = tempOutputPath + "/part-r-00000";
            tempOutputPath = tempPath + counter;
            Job i_job = prAdjustConfig(conf,tempInputPath, tempOutputPath, counter);
            i_job.waitForCompletion(true);

        }
        tempInputPath = tempOutputPath + "/part-r-00000";


        // use the threshold to find

        Job job = Job.getInstance(conf, "ThresholdProcess");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(ThresholdProcessMapper.class);
        job.setReducerClass(ThresholdReducers.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(tempInputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);


//
//        Job job = Job.getInstance(conf, "page rank");
//        FileInputFormat.addInputPath(job, new Path(inPath));
//        FileOutputFormat.setOutputPath(job, new Path(outPath));
//
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}