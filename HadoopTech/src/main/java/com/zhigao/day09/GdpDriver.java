package com.zhigao.day09;



import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GdpDriver {


    /**
     * "^[0-9]*[1-9][0-9]*$"
     *
     */
    //北京市,500,900,300,300=?bean
    public static class GdpStep1Mapper extends Mapper<LongWritable,Text,GdpBean,NullWritable> {

        GdpBean gdpBean=new GdpBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String format="^[0-9]*[1-9][0-9]*$";


            if(split.length==5  ){
                boolean matches = split[1].matches(format);
                boolean matches1 = split[2].matches(format);
                boolean matches2 = split[3].matches(format);
                boolean matches3 = split[4].matches(format);
                if(split.length==5 && matches && matches1 && matches3 && matches2   ) {
                    gdpBean.setGdpBean(split[0],Long.parseLong(split[1]),Long.parseLong(split[2]),Long.parseLong(split[3]),Long.parseLong(split[4]));
                    context.write(gdpBean,NullWritable.get());
                }

            }

        }
    }

    public static class  GdpStep1Reduce extends Reducer<GdpBean,NullWritable,GdpBean,NullWritable> {

        @Override
        protected void reduce(GdpBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            long sumTotal=0;
            Iterator<NullWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                iterator.next();
                sumTotal+=key.getSumSensor();
            }
            key.setSumSensor(sumTotal);
            context.write(key,NullWritable.get());
        }
    }

    /**
     * 将按着新的格式进行转化
     */
    public static class GdpStep2Mapper extends Mapper<LongWritable,Text,GdpBean,NullWritable> {
        GdpBean gdpBean=new GdpBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            gdpBean.setProvince(split[0]);
            gdpBean.setSumSensor(Integer.parseInt(split[1]));
            context.write(gdpBean,NullWritable.get());


        }
    }

    public static class  GdpStep2Reduce extends Reducer<GdpBean,NullWritable,Text,NullWritable> {
        Text keynew=new Text();

        @Override
        protected void reduce(GdpBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable c:
                    values) {
                String vlaue=   key.getProvince()+"\t"+key.getSumSensor();
                keynew.set(vlaue);
                context.write(keynew,NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(GdpDriver.class);
                //对输入输出参数设置
                // Text, IntWritable
        job.setGroupingComparatorClass(GdpGroupCompaetor.class);



                job.setOutputKeyClass(GdpBean.class);
                job.setOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(GdpBean.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(GdpStep1Mapper.class);
                job.setReducerClass(GdpStep1Reduce.class);



        job.setPartitionerClass(GdpPartition.class);

                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day09\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day09\\output"));
                boolean b = job.waitForCompletion(true);
              if(b){


                  Job job1 = Job.getInstance(cfg) ;
                  job1.setJarByClass(GdpDriver.class);
                  //对输入输出参数设置
                  // Text, IntWritable
                  job1.setSortComparatorClass(GdpsortCompartor.class);

                  job1.setOutputKeyClass(Text.class);
                  job1.setOutputValueClass(NullWritable.class);

                  job1.setMapOutputKeyClass(GdpBean.class);
                  job1.setMapOutputValueClass(NullWritable.class);
                  //设置map reduce类
                  job1.setMapperClass(GdpStep2Mapper.class);
                  job1.setReducerClass(GdpStep2Reduce.class);




                  //设置输入输出路径
                  FileInputFormat.setInputPaths(job1,new Path("C:\\HadoopTeacher\\data\\day09\\output"));
                  FileOutputFormat.setOutputPath(job1, new Path("C:\\HadoopTeacher\\data\\day09\\output1"));
                  boolean b1 = job1.waitForCompletion(true);
                  System.exit( b1== true?0:-1);

              }

    }
}
