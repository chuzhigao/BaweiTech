package com.zhigao.month.train2;


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
public class TestDriver {

//  2019-11-19	0010	07299	京W05104	2019-11-19 10:10:10	53	45	04
//2019-11-19,0012,08693,深Z31218,2019-11-19 10:10:09,117,33,04
        public static class FilmMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] c =   value.toString().split(",");
             if(c.length==8 && Integer.parseInt(c[5])>10 && Integer.parseInt(c[5])<200 ){
                context.write(value,NullWritable.get());

             }
            }
        }

        public static class FileReduce extends Reducer<Text,NullWritable,Text,NullWritable>  {

            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                context.write(key,NullWritable.get());
            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(TestDriver.class);
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
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month02"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\output22"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }
}
