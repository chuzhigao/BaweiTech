package com.zhigao.test.week21;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;


public class HashSetDriver {

	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml
        		Job job = Job.getInstance(conf);
        		job.setJobName("jobsetp1");
        		job.setJarByClass(HashSetDriver.class);
        		job.setMapperClass(Step1Mapper.class);
        		job.setReducerClass(Step1Recuce.class);
        		job.setMapOutputKeyClass(Text.class);
        		job.setMapOutputValueClass(NullWritable.class);
        		job.setOutputKeyClass(Text.class);
        		job.setOutputValueClass(NullWritable.class);

//        		FileInputFormat.setInputPaths(job, new Path(args[0]));
//        		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\test\\week21\\input"));
//		FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week21\\output11"));

		job.waitForCompletion(true) ;



    }
}
