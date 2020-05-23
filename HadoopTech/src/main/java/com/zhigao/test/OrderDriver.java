package com.zhigao.test;

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

import java.io.IOException;

public class OrderDriver {


    // 0000001,Pdt_01,222.8
    public static class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

        OrderBean orderBean = new OrderBean() ;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            orderBean.setOrderBean( Integer.parseInt(split[0]) , Double.parseDouble(split[2]) );
            // output the context
            context.write(orderBean, NullWritable.get());
        }
    }

    public static class  OrderReduce extends Reducer<OrderBean, NullWritable,OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    //job 进行设置
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(OrderDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(OrderBean.class);
                job.setOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(OrderMapper.class);
                job.setReducerClass(OrderReduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day04\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day04\\output1"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);



    }
}
