package com.zhigao.test.week2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * zhangsan-90
 * zhangsan,80
 *
 */
public class WeekTest2Mapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text name  = new Text() ;
    IntWritable num = new IntWritable(0) ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split ;
        boolean contains = s.contains("-");
        if(contains == true)
        {
            split = s.split("-");
        }
        else {
            split = s.split(",") ;
        }

        name.set(split[0]);
        num.set(Integer.parseInt(split[1]));

        context.write(name,  num);
    }



}
