package com.zhigao.telepflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartiton extends Partitioner<TeleptoneBean,NullWritable> {
    @Override
    public int getPartition(TeleptoneBean teleptoneBean, NullWritable nullWritable, int i) {
        int iPartition=0;
        String wordStr = teleptoneBean.getTelephone().substring(0,3);
        if( wordStr.equals("135")){
            iPartition=0;
        }
       else if( wordStr.equals("136")){
            iPartition=1;
        }

        else {
            iPartition=2;
        }

        return iPartition;

    }
//    @Override
//    public int getPartition(Text text, IntWritable intWritable, int i) {
//        int iPartition=0;
//        String wordStr = text.toString().substring(0,3);
//        if( wordStr.equals("135")){
//            iPartition=0;
//        }
//       else if( wordStr.equals("136")){
//            iPartition=1;
//        }
//
//        else {
//            iPartition=2;
//        }
//
//        return iPartition;
//    }
}
