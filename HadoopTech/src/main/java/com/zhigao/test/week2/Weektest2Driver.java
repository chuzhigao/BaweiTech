package com.zhigao.test.week2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Weektest2Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml

        		Job job = Job.getInstance(conf);

        		job.setJarByClass(Weektest2Driver.class);

        		job.setMapperClass(WeekTest2Mapper.class);
        		job.setReducerClass(WeekTest2Reduce.class);

        		job.setMapOutputKeyClass(Text.class);
        		job.setMapOutputValueClass(IntWritable.class);

        		job.setOutputKeyClass(Text.class);
        		job.setOutputValueClass(IntWritable.class);

//        		FileInputFormat.setInputPaths(job, new Path(args[0]));
//        		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\test\\week2\\one\\input"));
//		FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week2\\one\\outp1ut"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.waitForCompletion(true);
    }
}
