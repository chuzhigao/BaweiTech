package com.zhigao.day09;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class GdpPartition extends Partitioner<GdpBean,NullWritable> {

    @Override
    public int getPartition(GdpBean gdpBean, NullWritable nullWritable, int i) {

        return (gdpBean.getProvince().hashCode() & Integer.MAX_VALUE )%i;
    }
}
