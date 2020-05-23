package com.zhigao.day04;

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
import java.util.ArrayList;
import java.util.Collections;

public class TopSortDriver {

    /**
     *  0000001	Pdt_01	222.8
     *  要将数据 转化为自定义二次排序的类bean (orderid,price )
     */
    public static  class  TopSortMapper extends Mapper<LongWritable,Text, OrderBean ,NullWritable> {
        //定义一个bean
        OrderBean orderBean = new OrderBean();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 0000001,Pdt_01,222.8
            //其中正则表达式  "^\\d+(\\.\\d+)?$"
            String format = "^\\d+(\\.\\d+)?$" ;
            String[] split = value.toString().split(",");

          //  if( split.length==3 &&  split[2].matches(format)){
                orderBean.setOrderBean(Integer.parseInt(split[0]), Double.parseDouble(split[2]));

                //将对象输出到上下文
                context.write(orderBean, NullWritable.get() );
           // }




 }
    }

    /**
     * 将来reduce 过程中 会把相同的组的数据汇总到一起，按订单号进行reduce
     * 如果需要取按分组取多少名，需要进行一次过滤
     */
    public static class  TopSortReduce extends Reducer< OrderBean, NullWritable, OrderBean,NullWritable> {

        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key,NullWritable.get());

        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(TopSortDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(TopSortMapper.class);
                job.setReducerClass(TopSortReduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day04\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day04\\inpu1tou1t1111"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);



    }
}
