package com.zhigao.day08;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class Step1Mapper  extends Mapper<LongWritable , Text , Text, IntWritable> {
  // 减少内存开销
    String filename ="" ;
    IntWritable number = new IntWritable(1) ;
    Text word = new Text() ;

    // context 上下文信息保存setup  去可以提前获取


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName() ;

    }

    //  map 过程
   // hello--as.txt 1 atguigu pingping => atguigu--a.txt 1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        for (String strWord : s) {
           String kw = strWord + "--"+filename ;
           word.set(kw);
           //
            context.write(word, number);
        }
    }
}
