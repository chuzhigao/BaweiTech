package com.zhigao.test.week30;


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

public class LogDriver {


    // map 转化过程
    public static class FilmMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        Text keynew = new Text() ;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //2012-01-01 12:31:12,User01,211.167.248.22,eecf0780-2578,1,GET/top
            String[] split = value.toString().split(",");
            if(split.length==6){

                String valueNew = split[0] +"\t" + split[1]+"\t" +split[2]+"\t" +split[3]+"\t" +split[4];
                String url = split[5].substring(4);
                if(url.equals("top")){
                    keynew.set(valueNew + "\t" + "/"+ url);
                    context.write(keynew,NullWritable.get());
                }
            }
        }
    }


    //reduce 过程
    public static class FileReduce extends Reducer<Text,NullWritable,Text,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration cfg = new Configuration() ;
        //获取到任务
        Job job = Job.getInstance(cfg) ;
        job.setJarByClass(LogDriver.class);
        //对输入输出参数设置
        // Text, IntWritable

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置map reduce类
        job.setMapperClass(LogDriver.FilmMapper.class);
        job.setReducerClass(LogDriver.FileReduce.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week30\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week30\\outinput"));
        boolean b = job.waitForCompletion(true);
        System.exit( b== true?0:-1);

    }
}
