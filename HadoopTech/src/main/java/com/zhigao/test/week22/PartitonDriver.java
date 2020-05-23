package com.zhigao.test.week22;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitonDriver {

    public static class MobilReduce extends Reducer< Text, NullWritable , Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class MobileMapper extends Mapper<LongWritable , Text, Text , NullWritable> {

        Text mobil = new Text() ;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] s = value.toString().split(" ");
            for (String  c:
                 s ) {
                mobil.set(c);
                context.write( mobil, NullWritable.get()); ;

            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml

        		Job job = Job.getInstance(conf);

        		job.setJarByClass(PartitonDriver.class);

        		job.setMapperClass(MobileMapper.class);
        		job.setReducerClass(MobilReduce.class);

        		job.setPartitionerClass(MobilePartitioner.class);

        		job.setNumReduceTasks(4);

        		job.setMapOutputKeyClass(Text.class);
        		job.setMapOutputValueClass(NullWritable.class);

        		job.setOutputKeyClass(Text.class);
        		job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        		FileInputFormat.setInputPaths(job, new Path("C:\\HadoopTeacher\\data\\test\\week22\\input"));
//        		FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week22\\input1"));

        		job.waitForCompletion(true);
    }
}
