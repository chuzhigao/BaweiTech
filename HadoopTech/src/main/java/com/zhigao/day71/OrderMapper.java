package com.zhigao.day71;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean order = new OrderBean();


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length == 3) {
            order.setId(fields[0]);
            order.setPid(fields[1]);
            order.setAmount(Integer.parseInt(fields[2]));
            order.setPname("");
        } else {
            order.setPid(fields[0]);
            order.setPname(fields[1]);
            order.setId("");
            order.setAmount(0);
        }

        context.write(order, NullWritable.get());
    }
}
