package com.zhigao.com.bawei;

import com.zhigao.day01.WordCountMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordFileDriver {

    // woard + fileneame => 次数

    public static class WordFile1Mapper extends Mapper<LongWritable,Text, Text,LongWritable> {
        Text  filename = new Text() ; //保存我们的切片的文件名信息
        Text  wordT = new Text() ;//开辟输出的单词空间。节省内存的开销
        LongWritable numT = new LongWritable(1);
        //预处理
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();

            filename.set(name);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(",");
            for (String c:
                  split) {
               String wordFile =  c + "->" + filename.toString();
                wordT.set(wordFile);
               context.write( wordT,numT );
            }
        }
    }

    public static class WordFile1Reduce extends Reducer<Text,LongWritable, Text,LongWritable> {
        LongWritable sumTotalL = new LongWritable(0) ;
        
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
         long  sumtotal =0 ;
            for (LongWritable wordnum:
                 values ) {
                sumtotal += wordnum.get();
            }
            sumTotalL.set(sumtotal);
            context.write(key, sumTotalL);
        }
    }

    //  atguigu->a.txt  10 => atguigu   a.txt=>10
    public static class WordFile2Mapper extends Mapper<LongWritable,Text, Text,Text> {
        Text keyT = new Text();
        Text valueT = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("->");
            keyT.set(split[0]);
            String[] split1 = split[1].split("\t");
            String valuenew = split1[0]+"=>"+split1[1] ;
            valueT.set(valuenew);
            context.write( keyT,valueT);

        }
    }

    // atguigu   a.txt=>10
    // atguigu   b.txt=>9    =>atguigu   a.txt=>10  b.txt=>9
    public static class WordFile2Reduce extends Reducer<Text,Text, Text,Text> {
        Text cc = new Text() ;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer() ;
            for (Text wordFile:
                 values) {
                buffer.append( wordFile).append("\t") ;
            }
            cc.set(buffer.toString());
            context.write(key, cc);

        }
    }


    //搭架子

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(WordFileDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(LongWritable.class);
                //设置map reduce类
                job.setMapperClass(WordFile1Mapper.class);
                job.setReducerClass(WordFile1Reduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day08\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day08\\input1"));
                boolean b = job.waitForCompletion(true);
                if(b==true)
                {

                    //获取到任务
                    Job job2 = Job.getInstance(cfg) ;
                    job2.setJarByClass(WordFileDriver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job2.setOutputKeyClass(Text.class);
                    job2.setOutputValueClass(Text.class);

                    job2.setMapOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);
                    //设置map reduce类
                    job2.setMapperClass(WordFile2Mapper.class);
                    job2.setReducerClass(WordFile2Reduce.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job2,new Path("C:\\HadoopTeacher\\data\\day08\\input1"));
                    FileOutputFormat.setOutputPath(job2, new Path("C:\\HadoopTeacher\\data\\day08\\input11"));
                    boolean b1 = job2.waitForCompletion(true);
                    System.exit( b1== true?0:-1);
                }




    }

}
