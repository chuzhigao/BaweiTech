package com.zhigao.day08;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step1Reduce  extends Reducer<Text, IntWritable,Text, IntWritable>  {
    IntWritable sumTotal = new IntWritable(0) ;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
        for (IntWritable num :
              values) {
            sum += num.get() ;
        }
        sumTotal.set(sum);
        context.write(key, sumTotal);
    }
}
