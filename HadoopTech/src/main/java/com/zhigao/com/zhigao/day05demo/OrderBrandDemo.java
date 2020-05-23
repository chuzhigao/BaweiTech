package com.zhigao.com.zhigao.day05demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.io.IOException;
import java.util.Iterator;

/**
 * 订单的数据
 * 1001,01,1
 * 品牌数据
 * 01,xiaomi
 *
 */
public class OrderBrandDemo {


    // map
    public static class  OrderBrandMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
        Text fileName = new Text() ;
        OrderBean orderBean = new OrderBean();
      //预处理：所有的mapreduce

        @Override
        // 所有mapreduce 过程中的预处理，能把很多的信息保存到上下文中，作为一个mapreduce 的类的局部变量
        //给别人调用。
        protected void setup(Context context) throws IOException, InterruptedException {
             FileSplit cc  = (FileSplit) context.getInputSplit();
             //预装载
            String name = cc.getPath().getName();
            fileName.set(name);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // 把所有的数据转化成bean
            String[] splitData = value.toString().split(",");
//            if(splitData.length==3 ){
            System.out.printf(" !!!!!!!!!!!!!!!!!" + fileName.toString());
            // order.txt
            //以个数为基准去判断文件属于那个，不准确，为了考试
    //                if( splitData.length==3 ){
                    //以hdfs 切片原理去获取保存在namenode中的元数据【文件名】 去判断改数据属于那个文件块
                    if( splitData.length==3 ){


                        // * 1001,01,1
                orderBean.setOrderId(splitData[0]);
                orderBean.setBrandId(splitData[1]);
                orderBean.setOrderNum(Integer.parseInt(splitData[2]));
                orderBean.setBrandName("");
                context.write(orderBean, NullWritable.get());

            }else
            {
                orderBean.setBrandId(splitData[0]);
                orderBean.setBrandName(splitData[1]);
                orderBean.setOrderNum(0);
                orderBean.setOrderId("");
                context.write(orderBean, NullWritable.get());

            }


        }
    }

    // reduce
    public static  class  OrderBrandReduce extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {

        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//          // 数据结构给大家画了【bean1[品牌] bean2[order1] , bean3[order2]】
//            Iterator<NullWritable> iterator = values.iterator();
//            iterator.next() ;
//            OrderBean bean = new OrderBean();
//            bean.setBrandName( key.getBrandName());
//            //循环得到所有订单的bean ,要替换一下
//            while (iterator.hasNext())
//            {
//
//                iterator.next();
//                key.setBrandName(bean.getBrandName());
//                context.write(key,NullWritable.get());
//            }
            //测试结果
            for (NullWritable d:
                 values) {
                context.write(key,NullWritable.get());
            }


        }
    }

    //job 的配置
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //客户端
        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(OrderBrandDemo.class);

                job.setGroupingComparatorClass(OrderGroupByCompartor.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(OrderBean.class);
                job.setOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(OrderBean.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(OrderBrandMapper.class);
                job.setReducerClass(OrderBrandReduce.class);
                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day07\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day07\\chuzhigao"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);

    }



}
