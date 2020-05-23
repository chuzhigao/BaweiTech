package com.zhigao.day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class WordCountJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration  cfg = new Configuration() ;
        //获取到任务
        Job job = Job.getInstance(cfg) ;
        job.setJarByClass(WordCountJob.class);
        //对输入输出参数设置
        //对切片文件类型改变
        //可以讲各种小文件进行合并
//        job.setInputFormatClass(CombineFileInputFormat.class);
//        //其中1M =1024*1024 字节
//        CombineFileInputFormat.setMaxInputSplitSize(job,4194304);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置map reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);




        //设置输入输出路径
//        FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day01\\input"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day01\\output111111"));
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit( b== true?0:-1);

    }
}
