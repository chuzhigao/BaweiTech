package com.zhigao.day06;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
 * map => Bean(telphoen,upFlow,downFlow,sumFlow)
 * (Long ,Text ) => (Text  , Bean )
 * reuduce  Bean 求和
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
   //返回的bean在类成员变量中给开辟一个新的内存地址，减少内存开销
    FlowBean flowBean = new FlowBean() ;
    Text telp = new Text() ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // 读取数据，进行切分，然后给ＢＥＡＮ赋值
        String[] telepInfo = value.toString().split("\t");
        flowBean.setFlowBean( telepInfo[1] ,   Long.parseLong( telepInfo[telepInfo.length -3]) ,  Long.parseLong( telepInfo[telepInfo.length -2])  );
       // 输出到上线文中　
        telp.set(telepInfo[1]);
        context.write(telp, flowBean);
    }
}
