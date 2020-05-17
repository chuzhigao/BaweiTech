package com.zhigao.day03;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * 第一步 会将所有的相同key  手机的信息按key 进行map
 * 第二步：将相同的key 值的上下行流量及合计汇总到一起
 *        比如
 *        21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
 *        22 	13568436656	192.168.100.19			        1116	954	    200
 *        key:  13568436656   bean1(13568436656,2481,24681) bean2(13568436656,1116,954)
 */

public class FlowReduce extends Reducer<Text, FlowBean, FlowBean, NullWritable> {

    // 局部变量输出的合计
    FlowBean flowBean = new FlowBean() ;


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long downFlowSum =  0 ; //下行合计
        long upFlowSum =0 ;    // 上行合计
        for ( FlowBean c: values) {
          //取所有的上行 总计 ，下行总计
         downFlowSum += c.getDownFlow()  ;
         upFlowSum += c.getUpFlow() ;
        }
        //将汇总后的数据写出到上线文输出
        flowBean.setFlowBean(key.toString() , upFlowSum, downFlowSum);
        context.write(flowBean, NullWritable.get());
    }
}
