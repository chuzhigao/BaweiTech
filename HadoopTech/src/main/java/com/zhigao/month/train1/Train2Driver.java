package com.zhigao.month.train1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用MapReduce对以上数据进行过滤，只留高压超过140并且低压小于60之间的记录，
 * 并将其保存到HDFS的/car/logs目录下。其中，编写MR过滤代码逻辑（5分），
 * 运行程序并输出结果到HDFS
 * 所有的數據清晰過濾就是帥選
 * 所以其中输入输出字段[Text , null , Text , null ]
 *
 */
public class Train2Driver {


        public static class FilmMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 2019-11-19,0001,02009,2019-11-19 11:55:37,174,95,08
                String[] split = value.toString().split(",");
                int heightBlood = Integer.parseInt( split[4]) ;
                int shootBlood = Integer.parseInt( split[5]) ;
                if(split.length == 7 && heightBlood >=140 && shootBlood <=60){

                    context.write( value , NullWritable.get());
                }
            }
        }

        public static class FileReduce extends Reducer<Text,NullWritable,Text,NullWritable>  {

            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

                context.write(key, NullWritable.get());
            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Train2Driver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(NullWritable.class);

                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(NullWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper.class);
                    job.setReducerClass(FileReduce.class);


                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month012"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }
}
