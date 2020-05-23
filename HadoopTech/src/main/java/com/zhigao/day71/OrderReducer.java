package com.zhigao.day71;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        while (values.iterator().hasNext())
//        {
//            values.iterator().next() ;
//            System.out.println("first name =" + key.getPname() + " good id =" +  key.getPid() + " : sid " + key.getId() + "  num =" + key.getAmount());
////
//        }
//
//





        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        System.out.println("first name =" + key.getPname() + " good id =" + key.getPid() + " : sid " + key.getId() + "  num =" + key.getAmount());

        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            System.out.println(" 22222222 name =" + key.getPname() + " good id =" + key.getPid() + " : sid " + key.getId() + "  num =" + key.getAmount());

            context.write(key, NullWritable.get());
        }
    }
}