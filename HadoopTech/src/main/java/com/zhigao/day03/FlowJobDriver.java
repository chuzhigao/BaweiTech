package com.zhigao.day03;

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

        //启动job进程,等待任务执行完毕
        FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\day03\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day03\\output"));

        boolean b = job.waitForCompletion(true);

        //根据结果返回相应处理 0 正常退出，1 非正常退出
        System.exit(b==true?0:1);


    }
}
