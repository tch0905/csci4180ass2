import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class PRAdjust {

//    public static Double total = 0.0;

;

    public static class PRAdjustMapper extends Mapper<Object, Text, Text, PRNodeWritable> {



//        @Override
//        public void setup(Context context) {
//            total  = 0.0;
//        }

        ArrayList<PRNodeWritable> list = new ArrayList<PRNodeWritable>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // Extract node information from the text
            Integer nodeId = Integer.parseInt(tokens[1]);
            double pagerank = Double.parseDouble(tokens[2]);
            boolean isNode = Boolean.parseBoolean(tokens[3]);
            List<Integer> adjacencyList = new ArrayList<Integer>();

            for(int i=4; i<tokens.length; i++){
                adjacencyList.add(Integer.parseInt(tokens[i]));
            }


//            total += pagerank;

            PRNodeWritable node = new PRNodeWritable(nodeId, pagerank, isNode);
            node.setAdjList(adjacencyList);


            // Emit the node
            context.write(new Text(nodeId.toString()), node);
//            list.add(value);
        }

//        public void cleanup(Context context) throws IOException, InterruptedException {
//            total = 0.0;
//        }

    }



//        public void cleanup(Context context) throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            Double alpha = Double.parseDouble(conf.get("alpha"));
//
//            for (PRNodeWritable val : list) {
//                Double adjustPageRank = alpha * (1.0 / nodeCounter) +
//                        (1 - alpha) * ((1 - total) / nodeCounter + val.getP());
//                val.setP(adjustPageRank);
//                context.write(new Text(), val);
//
//            }
////            context.write(new IntWritable(0), new PRNodeWritable(0, new DoubleWritable(mm), false));
//        }


    public static class PRAdjustReducer extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable> {

        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {


            Configuration conf = context.getConfiguration();
            Double alpha = Double.parseDouble(conf.get("alpha"));
//            Double total = 1.0;
            Double total =Double.parseDouble(conf.get("totalP")) ;
//            total /= 1000000000;
            System.out.print("total equal");
            System.out.println(total);
            Long totalNode = Long.parseLong(conf.get("totalNode"));
//            Long totalNode = 6l;
            System.out.print("totalNode equal");
            System.out.println(totalNode);

            for (PRNodeWritable val : values) {
                Double adjustPageRank = alpha * (1.0 / totalNode) +
                        (1 - alpha) * ((1 - total) / totalNode + val.getP());

                val.setP(adjustPageRank);
                context.write(key, val);
            }
        }
    }




    public static Job prAdjust(Configuration conf, String inPath, String outPath, int i) throws Exception {

        Job job = Job.getInstance(conf, "prAdjust " + i);

        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(PRAdjustMapper.class);
        job.setReducerClass(PRAdjustReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}
