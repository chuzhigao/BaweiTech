package com.zhigao.day71;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());

    job.setJarByClass(OrderDriver.class);
    job.setMapperClass(OrderMapper.class);
    job.setReducerClass(OrderReducer.class);
    job.setGroupingComparatorClass(OrderCompatator.class);

    job.setMapOutputKeyClass(OrderBean.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(OrderBean.class);
    job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day07\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day07\\output"));


//
//    FileInputFormat.setInputPaths(job, new Path("C:\\ahadoop\\07\\joinOrderUser\\input"));
//    FileOutputFormat.setOutputPath(job, new Path("C:\\ahadoop\\07\\joinOrderUser\\output"));


    boolean b = job.waitForCompletion(true);

    System.exit(b ? 0 : 1);
}

}
