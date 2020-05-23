package com.zhigao.day06;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SfPartiton  extends Partitioner<Text, FlowBean > {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        int ip =0 ;
        String sf = text.toString().substring(0, 3);
        if(sf.equals("136")){
            ip = 0;
        }
        else if(sf.equals("137")){
            ip = 1;
        }else if(sf.equals("138")){
            ip = 2;
        }else if(sf.equals("139")){
            ip = 3;
        }else {
            ip = 4;
        }
        return ip;

    }
}
