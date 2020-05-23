package com.zhigao.day08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Step2Driver {
  // 内部内map  atguigu--a.txt	3 atguigu a.txt-3 bb.txt-3
  public static class  Step2Mapper extends Mapper<LongWritable , Text, Text, Text> {
      Text outTx = new Text() ;
      Text keys = new Text() ;
      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] split = value.toString().split("--");
          String[] split1 = split[1].split("\t");
          //输入输出值都有了
          outTx.set(split1[0] + "->" + split1[1]);
          keys.set(split[0]);

          context.write(keys, outTx);
          }



  }
    public static  class  Step2Reduce  extends Reducer<Text, Text, Text, Text> {

        Text outResule = new Text() ;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // string buffer
            StringBuffer outStringBuffer = new StringBuffer() ;
            for ( Text wd : values ) {
                outStringBuffer.append( wd.toString()).append( "\t") ;

            }
            outResule.set(outStringBuffer.toString());
            context.write(key, outResule);
        }
    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

      Configuration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml

      		Job job = Job.getInstance(conf);

      		job.setJarByClass(Step2Driver.class);

      		job.setMapperClass(Step2Mapper.class);
      		job.setReducerClass(Step2Reduce.class);





      		job.setMapOutputKeyClass(Text.class);
      		job.setMapOutputValueClass(Text.class);

      		job.setOutputKeyClass(Text.class);
      		job.setOutputValueClass(Text.class);

      		FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\day08\\output1"));
      		FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day08\\output2"));

      		job.waitForCompletion(true);

    }

}
