package com.zhigao.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {

    IntWritable sumTotal = new IntWritable(0) ;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalNum = 0 ;
        for ( IntWritable c:
              values) {
             totalNum += c.get() ;
        }
        sumTotal.set(totalNum);
        context.write(key, sumTotal);

    }
}
