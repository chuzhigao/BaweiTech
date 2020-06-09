package com.zhigao.month.train1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 2018-8-18	六	10.3.9.18	钱八	6	error
 * 思路：就是一個單詞統計：衹是將單詞換成了日期
 * 弄清輸入輸出【日期 text ,次數 int 】
 */
public class Train1Driver {


        public static class FilmMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

            Text keyDate = new Text() ;
            IntWritable num = new IntWritable(1) ;
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split(",");
                if(split.length == 6 ){
                    keyDate.set(split[0]);
                    context.write( keyDate, num);
                }
            }
        }

        public static class FileReduce extends Reducer<Text,IntWritable,Text,IntWritable>  {
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sumTotal = 0;
                for (IntWritable c:
                     values) {
                    sumTotal += c.get() ;
                }
                context.write(key, new IntWritable(sumTotal));

            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Train1Driver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(IntWritable.class);

                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(IntWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper.class);
                    job.setReducerClass(FileReduce.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month01"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }
}
