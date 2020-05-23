package com.zhigao.test.week22;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MobilePartitioner extends Partitioner<org.apache.hadoop.io.Text, NullWritable> {


    /**
     * // 130、131、132、133、134是联通，135、136、137、138、139是移动
     * @param text
     * @param nullWritable
     * @param i
     * @return
     */
    @Override
    public int getPartition(org.apache.hadoop.io.Text text, NullWritable nullWritable, int i) {
        String substring = text.toString().substring(0, 3);
        if("130".equals(substring) || "131".equals(substring)  || "132".equals(substring) || "133".equals(substring) || "134".equals(substring))
        {
            i = 0 ;
        }
        else if("135".equals(substring) || "136".equals(substring)  || "137".equals(substring) || "138".equals(substring) || "139".equals(substring))
        {
            i = 1 ;
        }
        else {
            i = 2 ;
        }
        return i;
    }
}
