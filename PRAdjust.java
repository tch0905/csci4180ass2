//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//import java.util.*;
//
//public class PRAdjust {
//
//    @Override
//    public void setup(Context context) {
//        isFirst = Boolean.parseBoolean(context.getConfiguration().get("isFirst"));
//        totalNode = Long.parseLong(context.getConfiguration().get("totalNode"));
//    }
//
//    public static class PRAdjustMapper extends Mapper<Object, Text, Text, PRNodeWritable> {
//
//
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String[] tokens = value.toString().split("\\s+");
//
//            // Extract node information from the text
//            Integer nodeId = Integer.parseInt(tokens[1]);
//            double pagerank = Double.parseDouble(tokens[2]);
//            boolean isNode = Boolean.parseBoolean(tokens[3]);
//            List<Integer> adjacencyList = new ArrayList<Integer>();
//
//            // Extract adjacency list
//            for(int i=4; i<tokens.length; i++){
//                adjacencyList.add(Integer.parseInt(tokens[i]));
//            }
//            if(isFirstIte){
//                node.setP(1.0 /nodeNum);
//            }
//
//
//            PRNodeWritable node = new PRNodeWritable(nodeId, pagerank, isNode);
//            node.setAdjList(adjacencyList);
//
//
//            // Emit the node
//            context.write(new Text(nodeId.toString()), node);
//
//            // Emit contributions to each neighbor's PageRank
//            double contribution = pagerank / adjacencyList.size();
//            for (Integer neighbor : adjacencyList) {
//                context.write(new Text(neighbor.toString()), new PRNodeWritable(neighbor, contribution, false));
//
//            }
//
//        }
//    }
//
//    public static class PRAdjustReducer extends Reducer<Text , PRNodeWritable, Text, PRNodeWritable>{
//        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
//
//            double sum = 0;
//
//            // Create a new NodeWritable object with updated PageRank
//            PRNodeWritable newNode = new PRNodeWritable();
//
//            // Sum up contributions from neighbors
//            for (PRNodeWritable value : values) {
//
//                if (value.isNode()) {
//                    newNode.set(value);
//
//                }
//                else {
//                    sum += value.getP();
//                }
//            }
//
//            newNode.setP(sum);
//
//            // Emit the updated node
//            context.write(key, newNode);
//
//        }
//    }
//
//    public static Job prAdjust(Configuration conf, String inPath, String outPath, int i)throws Exception{
//
//        Job job = Job.getInstance(conf, "prAdjust "+ i);;
//        job.setJarByClass(PRAdjust.class);
//        job.setMapperClass(PRAdjustMapper.class);
//        job.setReducerClass(PRAdjustReducer.class);
//        job.setMapOutputKeyClass(Text .class);
//        job.setMapOutputValueClass(PRNodeWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(PRNodeWritable.class);
//        FileInputFormat.addInputPath(job, new Path(inPath));
//        FileOutputFormat.setOutputPath(job, new Path(outPath));
//        return job;
//    }
//
//}