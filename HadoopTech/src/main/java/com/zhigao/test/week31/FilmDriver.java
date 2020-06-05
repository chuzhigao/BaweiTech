package com.zhigao.test.week31;

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

public class FilmDriver {

    // 1 碟中谍-全面瓦解 欧美 土豆 1356
    public static class FilmMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        //Text keynew = new Text() ;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // data =>',' lengh =5  data[4]>800
            String[] split = value.toString().split(",");
            if(split.length==5){
                int i = Integer.parseInt(split[4]);
                if(i >800){
                    context.write(value, NullWritable.get());
                }
            }
        }
        }


    public static class FileReduce extends Reducer<Text,NullWritable,Text,NullWritable>  {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(FilmDriver.class);
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
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week31\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week31\\output1"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);

    }

}
