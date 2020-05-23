package com.zhigao.day07;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{

    OrderBean orderBean=new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据进行读取与转化
        //1001	01	1
        //01	小米
        String[] split = value.toString().split(",");
      //  System.out.printf("~~~~~~~~~~~~~"+split.length);
        //根据长度进行判断
        if(split.length==2){

            orderBean.setName(split[1]);
            orderBean.setUid(split[0]);
            orderBean.setNum(0);
            orderBean.setOid("");

         //   System.out.printf("!!!!!!!!!!!!!"+ split[0]);
            context.write(orderBean,NullWritable.get());
        }
         if(split.length==3) {
            orderBean.setName("");
            orderBean.setUid(split[1]);
            orderBean.setNum(Integer.parseInt(split[2]));
            orderBean.setOid(split[0]);

          //  orderBean.setOrderBean(,split[1],,"");
       //     System.out.printf("!!!!!!!!!!!!!!!!~~~~~~~~~~~~~~~"+split[0]);
            context.write(orderBean,NullWritable.get());
        }

    }
}
