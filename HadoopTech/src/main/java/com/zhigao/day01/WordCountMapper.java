package com.zhigao.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 要将 hello you
 *      hello me
 *      hello everyboday
 *      =>hello 3
 *        you    1
 *        me     1
 * 1: map hello =>(hello 1)
 * 2:reduce       把相同的 hello 1 1 1 =>hello 3
 */
public class WordCountMapper extends Mapper<LongWritable , Text, Text, IntWritable> {

    //定义一个变量输出结果
    Text  keyout = new Text();
    IntWritable number = new IntWritable(1) ;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将数据进行单词切分
        String[] s = value.toString().split(" ");
        for (String word:
             s) {
            keyout.set(word);
            //循环将数据切分并且输出key value
            context.write(keyout, number);
        }

    }
}
