package com.zhigao.day07;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReduce extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
    OrderBean orderBean=new OrderBean();
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //第一个为品牌

        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getName();
        System.out.println("first name =" + key.getName() + " good id =" + key.getUid() + " : sid " + key.getOid() + "  num =" + key.getNum());

        while (iterator.hasNext()) {
            iterator.next();
            key.setName(pname);
            System.out.println(" 22222222 name =" + key.getName() + " good id =" + key.getUid () + " : sid " + key.getOid() + "  num =" + key.getNum());

            context.write(key, NullWritable.get());
        }


//        Iterator<NullWritable> iterator = values.iterator();
//        iterator.next();
//        OrderBean c =new OrderBean();
//        String name = key.getName();
//        String uid =key.getUid();
//        c.setName(name);
//        c.setUid(uid);
//        System.out.printf("name=!!!!!!!!!!!!!"+name);
//
//        while (iterator.hasNext()){
//            iterator.next();
//           // System.out.printf("child name=!!!!!!!!!!!!!"+c.getName()+"-"+c.getUid());
//            key.setName(name);
//
//            System.out.println(" 22222222 name =" + key.getUid() + " good id =" + key.getOid() + " : sid " + key.getName() + "  num =" + key.getNum());
//
//
//            context.write(key,NullWritable.get());
//        }
//        for (NullWritable c:
//             values) {
//
//            context.write(key,NullWritable.get());
//        }
    }
}
