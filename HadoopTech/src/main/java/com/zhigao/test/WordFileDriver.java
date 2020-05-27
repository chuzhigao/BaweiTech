package com.zhigao.test;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordFileDriver {
    public static  class  WordFileMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        Text filename=new Text();
        Text wordT=new Text();
        LongWritable num=new LongWritable(1);
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();
            filename.set(name);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String wordFile;
            for (String c:
                    split) {
                wordFile=c+"->"+filename.toString();
                wordT.set(wordFile);
                context.write(wordT,num);
            }
        }
    }
    public static class  WordFileReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        LongWritable sumTotal=new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sumtotal=0;
            for (LongWritable wordnum:
                    values){
                sumtotal+=wordnum.get();
            }
            sumTotal.set(sumtotal);
            context.write(key,sumTotal);
        }
    }
    public static  class  WordFile2Mapper extends Mapper<LongWritable,Text,Text,Text>{
        Text keyT=new Text();
        Text valueT=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("->");
            keyT.set(split[0]);
            String[] s = split[1].split("\t");
            String valuenew=split[0]+"=>"+split[1];
            valueT.set(valuenew);
            context.write(keyT,valueT);
        }
    }
    public static class  WordFile2Reduce extends Reducer<Text,Text,Text,Text>{
        Text text=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buffer=new StringBuffer();
            for (Text wordFile:
                    values) {
                buffer.append(wordFile).append("\t");
            }
            text.set(buffer.toString());
            context.write(key,text);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration cfg = new Configuration() ;
        Job job = Job.getInstance(cfg) ;
        // 2 设置jar加载路径
        job.setJarByClass(WordFileDriver.class);
        // 3 设置map和reduce类
        job.setMapperClass(WordFileMapper.class);
        job.setReducerClass(WordFileReduce.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //文件输入在那个地方
        FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day08\\input"));
        //文件输出在哪个地方
        FileOutputFormat.setOutputPath(job,new Path("C:\\HadoopTeacher\\data\\day08\\input1"));

        boolean b = job.waitForCompletion(true);
        if (true){

            Job j = Job.getInstance(cfg) ;
            // 2 设置jar加载路径
            j .setJarByClass(WordFileDriver.class);
            // 3 设置map和reduce类
            j .setMapperClass(WordFile2Mapper.class);
            j .setReducerClass(WordFile2Reduce.class);

            // 4 设置map输出
            j .setMapOutputKeyClass(Text.class);
            j .setMapOutputValueClass(Text.class);
            // 5 设置最终输出kv类型
            j .setOutputKeyClass(Text.class);
            j .setOutputValueClass(Text.class);

            //文件输入在那个地方
            FileInputFormat.setInputPaths(j,new Path("C:\\HadoopTeacher\\data\\day08\\input1"));
            //文件输出在哪个地方
            FileOutputFormat.setOutputPath(j,new Path("C:\\HadoopTeacher\\data\\day08\\input2"));
            boolean b1 = j.waitForCompletion(true);
            System.exit(b1==true?0:1);
        }
    }
}
