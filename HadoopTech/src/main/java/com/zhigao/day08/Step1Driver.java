package com.zhigao.day08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step1Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml
        		Job job = Job.getInstance(conf);

        		job.setJarByClass(Step1Driver.class);

        		job.setMapperClass(Step1Mapper.class);
        		job.setReducerClass(Step1Reduce.class);




        		job.setMapOutputKeyClass(Text.class);
        		job.setMapOutputValueClass(IntWritable.class);

        		job.setOutputKeyClass(Text.class);
        		job.setOutputValueClass(IntWritable.class);

        		FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\day08"));
        		FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day08\\output1"));

        		job.waitForCompletion(true);

    }
}
