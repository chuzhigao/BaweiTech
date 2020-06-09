package com.zhigao.day07;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(OrderDriver.class);
        //设置map reduce类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);

        job.setGroupingComparatorClass(OrderGroup.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);





                //设置输入输出路径
               // FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day07\\input"));
         FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month04"));

        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day07\\outputqqq"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);
    }
}
