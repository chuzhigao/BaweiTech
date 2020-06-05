package com.zhigao.day06;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowJobDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 进行配置
        Configuration cfg = new Configuration() ;

        // 设定jobclass ,mapclass, reduceclass
        Job job = Job.getInstance(cfg) ;

        // 设定map reduce 的输入输出参数类型
        job.setJarByClass(FlowJobDriver.class );
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        // 对文件的输入输出进行设定

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);
        //单独加入分区的业务逻辑
        job.setPartitionerClass(SfPartiton.class);
        //改变reducetask的数量，去对每一个给定的业务分区进行分析
        job.setNumReduceTasks(4);


        //启动job进程,等待任务执行完毕
        FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\day06\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day06\\output1"));

        //自定义
        boolean b = job.waitForCompletion(true);




        //根据结果返回相应处理 0 正常退出，1 非正常退出
        System.exit(b==true?0:1);


    }
}
