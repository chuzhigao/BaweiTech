package com.zhigao.test.week3_1;

import com.zhigao.day01.WordCountMapper;
import com.zhigao.day01.WordCountReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Tes1Driver {

    // 2012-01-01,12:31:12,User01,211.167.248.22,eecf0780-2578,1,GET/top
    //数据像上面那个样
    public static class  TestMapper extends Mapper<LongWritable, Text, Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if(split.length ==7 ){
                String substring = split[6].substring(4);
                if(substring.equals("top")){


                }


            }


            super.map(key, value, context);
        }
    }

    public static class TestReduce  extends Reducer<Text,NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(Tes1Driver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(TestMapper.class);
                job.setReducerClass(TestReduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day01\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day01\\output1"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);


    }



}
