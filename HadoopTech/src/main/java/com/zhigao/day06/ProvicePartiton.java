package com.zhigao.day06;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvicePartiton extends Partitioner<Text,FlowBean> {


    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String substring = text.toString().substring(0, 3);
        int ii=0;
        if(substring.equals("136")){
            ii=0;
        }else if (substring.equals("137")){
            ii=1;
        }else if (substring.equals("138")){
            ii=2;
        }else if (substring.equals("139")){
            ii=3;
        }
        else {
            ii=4;
        }

        return ii;
    }
}
