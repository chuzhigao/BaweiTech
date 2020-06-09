package com.zhigao.month.train2;
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

public class Test3Driver {

    // xiaoming,apple,90=>xiaoming,90
        public static class FilmMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

         String[] c =   value.toString().split(",");
         context.write( new Text(c[0]), new IntWritable(Integer.parseInt(c[2])));
        }
    }

        public static class FileReduce extends Reducer<Text,IntWritable,Text,IntWritable>  {
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int i =0;
                for (IntWritable c:

                    values ) { i += c.get();
                }
                context.write(key, new IntWritable(i));

            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Test3Driver.class);
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
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month03"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\outmonth03"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }
}
