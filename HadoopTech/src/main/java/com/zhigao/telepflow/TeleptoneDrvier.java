package com.zhigao.telepflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TeleptoneDrvier {

    // map 采用内部类去实现

    /**      tel         ip             url             up       down    status
     * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
     *
     */
    public static class TeleptoneMapper extends Mapper<LongWritable, Text, TeleptoneBean,NullWritable> {
        // map 做的方法
        TeleptoneBean teleptoneBean = new TeleptoneBean() ;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
            String[] split = value.toString().split("\t");
            //数据清洗，长度要求是7 个，对数据要求是整数
            // 对数据进行清洗按规则
            String foramte ="^[0-9]*[1-9][0-9]*$" ;
            if(split.length == 7 && split[ split.length -3 ].matches(foramte) && split[ split.length -2 ].matches(foramte)){
                teleptoneBean.setTeleptoneBean( split[1], Long.parseLong( split[ split.length -3 ] ), Long.parseLong( split[ split.length -2 ] )   );
                context.write(teleptoneBean, NullWritable.get());
            }


        }
    }


    //reduce

    /**
     *
     */
    public static class  TeleptoneReduce extends Reducer< TeleptoneBean,NullWritable ,TeleptoneBean,NullWritable > {

        TeleptoneBean teleptoneBean = new TeleptoneBean();

        @Override
        protected void reduce(TeleptoneBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //
            long sumUp = 0;
            long sumDown = 0;
            for (NullWritable c :
            values) {
                sumUp += key.getUpFlow() ;
                sumDown += key.getDownFlow() ;
            }
            teleptoneBean.setTeleptoneBean( key.getTelephone() , sumUp, sumDown );
            context.write(teleptoneBean, NullWritable.get());
        }
    }
    //job 工作完成流程
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(TeleptoneDrvier.class);
                //对输入输出参数设置
                // Text, IntWritable
                job.setPartitionerClass(WordPartiton.class);
                job.setNumReduceTasks(4);

                job.setOutputKeyClass(TeleptoneBean.class);
                job.setOutputValueClass(NullWritable.class);

                job.setMapOutputKeyClass(TeleptoneBean.class);
                job.setMapOutputValueClass(NullWritable.class);
                //设置map reduce类
                job.setMapperClass(TeleptoneMapper.class);
                job.setReducerClass(TeleptoneReduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day03\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day03\\output1222"));
                boolean b = job.waitForCompletion(true);
                System.exit( b== true?0:-1);


    }
}
