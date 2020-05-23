package com.zhigao.test.week2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeekTest2Reduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    IntWritable sum = new IntWritable(0) ;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0 ;
        for (IntWritable iscore :
             values ) {
            i += iscore.get() ;

        }
        sum.set(i);
        context.write(key, sum );
    }

}
