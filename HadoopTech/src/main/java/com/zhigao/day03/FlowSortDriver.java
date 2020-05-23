package com.zhigao.day03;

import com.zhigao.day01.WordCountJob;
import com.zhigao.day01.WordCountMapper;
import com.zhigao.day01.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 如果有多个业务逻辑需要进行多个mapreduce过程
 * 比如现在需要进行按流量的合计进行倒序排序
 * 要实现WRITABLECOMPARE的接口
 */
public class FlowSortDriver {



// 通过上一次结果进行转化为带排序的新的bean
    public  static class FlowSortMapper extends Mapper<LongWritable, Text,FlowBeanNew, NullWritable>{
        FlowBeanNew bean=new FlowBeanNew();
        @Override
        // 13470253144	180	180	360
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            bean.setBeanNew(split[0],Long.parseLong(split[1]),Long.parseLong(split[2]),Long.parseLong(split[3]) );
            context.write(bean,NullWritable.get());
        }
    }

    public static  class FlowSortReduce extends Reducer<FlowBeanNew,NullWritable,FlowBeanNew,NullWritable>{

        @Override
        protected void reduce(FlowBeanNew key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }

    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(FlowSortDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(FlowBeanNew.class);
                job.setMapOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(FlowBeanNew.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(FlowSortMapper.class);
                job.setReducerClass(FlowSortReduce.class);
                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day03\\output"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day03\\output1"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);
    }

}
