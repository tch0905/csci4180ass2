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

    public static class PRAdjustMapper extends Mapper<Object, Text, Text, PRNodeWritable>{

    }

    public static class PRAdjustReducer extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable>{

    }

    public static Job prAdjust(Configuration conf, String inPath, String outPath)throws Exception{

        Job job = Job.getInstance(conf, "PRAdjust");
        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(PRAdjustMapper.class);
        job.setReducerClass(PRAdjustReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }

}