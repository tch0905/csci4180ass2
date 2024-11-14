import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class PageRank {
    public enum COUNTER{
        TOTALNODE,
    }

    public static enum PageRankCounter {
        PAGERANK;
    }

    public static class ThresholdProcessMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");


            // Extract node information from the text
            String nodeId = tokens[1];
            Double pagerank = Double.parseDouble(tokens[2]);
            Configuration conf = context.getConfiguration();
            Double threshold = Double.parseDouble(conf.get("threshold"));
            if(pagerank>threshold) {
                context.write(new Text(nodeId), new DoubleWritable(pagerank));

            }

        }
    }

    public static class ThresholdReducers extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) throws IOException, InterruptedException {

//            Configuration conf = context.getConfiguration();
//            Double threshold = Double.parseDouble(conf.get("threshold"));
//            threshold = 2.0;
            for(DoubleWritable val: values){
//                if(pagerank>threshold) {
                    context.write(key, val);
//                }
            }
//            context.write(key, val);

        }
    }

    public static class PRMainLoopMapper extends Mapper<Object, Text, Text, PRNodeWritable> {
        private Boolean isFirst;
        private long totalNode;

        @Override
        public void setup(Context context) {
            isFirst = Boolean.parseBoolean(context.getConfiguration().get("isFirst"));
            totalNode = Long.parseLong(context.getConfiguration().get("totalNode"));
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // Extract node information from the text
            Integer nodeId = Integer.parseInt(tokens[1]);
            Double pagerank = Double.parseDouble(tokens[2]);
            boolean isNode = Boolean.parseBoolean(tokens[3]);
            List<Integer> adjacencyList = new ArrayList<Integer>();

            // Extract adjacency list
            for(int i=4; i<tokens.length; i++){
                adjacencyList.add(Integer.parseInt(tokens[i]));
            }

            PRNodeWritable node = new PRNodeWritable(nodeId, pagerank, isNode);
            node.setAdjList(adjacencyList);
            if(isFirst){
                node.setP(1.0 /totalNode);
                pagerank = 1.0 /totalNode;
            }


            // Emit the node
            context.write(new Text(nodeId.toString()), node);

            // Emit contributions to each neighbor's PageRank
            Double contribution = pagerank / adjacencyList.size();
            for (Integer neighbor : adjacencyList) {
                context.write(new Text(neighbor.toString()), new PRNodeWritable(neighbor, contribution, false));

            }

        }
    }

    public static class PRMainLoopReducers extends Reducer<Text , PRNodeWritable, Text, PRNodeWritable>{

        private Double totalPangRank =0.0;
        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {

            Double sum = 0.0;

            // Create a new NodeWritable object with updated PageRank
            PRNodeWritable newNode = new PRNodeWritable();

            // Sum up contributions from neighbors
            for (PRNodeWritable value : values) {

                if (value.isNode()) {
                    newNode.set(value);

                }
                else {
                    sum += value.getP();
                }
            }

            newNode.setP(sum);
//            totalPangRank += sum;
//            context.getCounter(PageRankCounter.PAGERANK).increment((long)(sum* 1000000000));

            // Emit the updated node
            context.write(key, newNode);

        }
//        public void cleanup(Context context) throws IOException, InterruptedException {
//            context.getConfiguration().set("total", totalPangRank.toString());
//        }

    }

    public static Job prMainLoop(Configuration conf, String inPath, String outPath, int i)throws Exception{

        Job job = Job.getInstance(conf, "prMainLoop "+ i);;
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMainLoopMapper.class);
        job.setReducerClass(PRMainLoopReducers.class);
        job.setMapOutputKeyClass(Text .class);
        job.setMapOutputValueClass(PRNodeWritable.class);
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


        Job preJob = PRPreProcess.preConfig(conf,inPath, tempOutputPath);
        preJob.waitForCompletion(true);
        long totalNode = preJob.getCounters().findCounter(COUNTER.TOTALNODE).getValue();
        conf.setLong("totalNode", totalNode);

        boolean isFirst = true;
        for (counter = 1; counter <= ite; counter++) {
            conf.setBoolean("isFirst", isFirst);
            isFirst = false;
            tempInputPath = tempOutputPath + "/part-r-00000";
            tempOutputPath = tempPath + counter;
            Job i_job = prMainLoop(conf,tempInputPath, tempOutputPath, counter);
            i_job.waitForCompletion(true);
            long totalP_long = i_job.getCounters().findCounter(PageRankCounter.PAGERANK).getValue();

//            Double totalP = (double)totalP_long;
//            totalP /= 1000000000;
//            System.out.println("totalP");
//            System.out.println(totalP);
//            System.out.println("totalP_long");
//            System.out.println(totalP_long);
//            conf.setDouble("totalP", totalP);
            tempInputPath = tempOutputPath + "/part-r-00000";
            tempOutputPath = tempPath + "/adjust/"+ counter;
            Job i_adjust = PRAdjust.prAdjust(conf,tempInputPath, tempOutputPath, counter);
            i_adjust.waitForCompletion(true);


        }
        tempInputPath = tempOutputPath + "/part-r-00000";


        // use the threshold to find

        Job job = Job.getInstance(conf, "ThresholdProcess");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(ThresholdProcessMapper.class);
        job.setReducerClass(ThresholdReducers.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(tempInputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);

    }

}