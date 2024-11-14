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

    public static class PRAdjustMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable>{
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
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
		IntWritable one = new IntWritable(1);
		context.write(one,node);
	}


//        ArrayList<PRNodeWritable> list = new ArrayList<PRNodeWritable>();

//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String[] tokens = value.toString().split("\\s+");
//
//            // Extract node information from the text
//            Integer nodeId = Integer.parseInt(tokens[1]);
//            Double pagerank = Double.parseDouble(tokens[2]);
//            boolean isNode = Boolean.parseBoolean(tokens[3]);
//            List<Integer> adjacencyList = new ArrayList<Integer>();
//
//            for(int i=4; i<tokens.length; i++){
//                adjacencyList.add(Integer.parseInt(tokens[i]));
//            }
//
//
////            total += pagerank;
//
//            PRNodeWritable node = new PRNodeWritable(nodeId, pagerank, isNode);
//            node.setAdjList(adjacencyList);
//
//
//            // Emit the node
//            context.write(new Text(nodeId.toString()), node);
////            list.add(value);
//        }

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


    public static class PRAdjustReducer
            extends Reducer<IntWritable,PRNodeWritable,Text,PRNodeWritable> {

	    private Double alpha;
	    private Integer number_node;
	    private Double mass = 0.0;
	    private List<PRNodeWritable> list;
        protected void setup(Context context)  throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.alpha = Double.parseDouble(conf.get("alpha"));
            this.number_node = Integer.parseInt(conf.get("totalNode"));
            list = new ArrayList<PRNodeWritable>();
                }

	public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		for(PRNodeWritable val : values){
//            if(val.getAdjList().isEmpty()){
//                mass+=val.getP();
//            }
            mass+=val.getP();


			PRNodeWritable tmp = new PRNodeWritable();
			tmp.set(val);
			list.add(tmp);
		}
	}
	 public void cleanup(Context context) throws IOException, InterruptedException {
        mass = 1.0 - mass;
        for(int i=0;i<this.list.size();i++){
        //context.write(NullWritable.get(),list.get(i));
            PRNodeWritable tmp_node = new PRNodeWritable();
            tmp_node.set(list.get(i));
            Double tmp_rank = tmp_node.getP();
            tmp_rank = alpha*(1.0/number_node) + (1.0-alpha)*(tmp_rank + mass/number_node);
            tmp_node.setP(tmp_rank);
            context.write(new Text(String.valueOf(tmp_node.getNodeId())),tmp_node);
		}
    }

    }




    public static Job prAdjust(Configuration conf, String inPath, String outPath, int i) throws Exception {

        Job job = Job.getInstance(conf, "prAdjust " + i);

        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(PRAdjustMapper.class);
        job.setReducerClass(PRAdjustReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}
