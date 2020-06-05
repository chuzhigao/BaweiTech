package com.zhigao.test.week33;
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

public class AvgDriver {



        public static class FilmMapper extends Mapper<LongWritable,Text,UserScoreBean,NullWritable> {

            UserScoreBean userScoreBean = new UserScoreBean() ;
            // 张三,98,0 => bean

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split(",");
                if(split.length ==3 ){
                    // String useName, int theroyScore, int techScore, int number, int theroyScoreSum, int techScoreSum, double theroyAvg, double techAvg
                    userScoreBean.setUserScoreBean(split[0],Integer.parseInt(split[1]), Integer.parseInt(split[2]),1 ,0,0,0,0);
                    context.write(userScoreBean, NullWritable.get());
                }
            }
        }

        public static class FileReduce extends Reducer<UserScoreBean,NullWritable,UserScoreBean,NullWritable> {

            UserScoreBean userScoreBean = new UserScoreBean();
            //对理论，技能 及总个数求合计 在求平均值
            @Override
            protected void reduce(UserScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int theryTotal = 0 ;
                int  techTotal = 0;
                int  numTotal = 0;
                for (NullWritable c:
                     values) {
                     theryTotal += key.getTheroyScore() ;
                     techTotal += key.getTechScore() ;
                     numTotal += key.getNumber() ;
                }
                //使用总分除以个数得到各自理论与技能的平均分
                key.setTechAvg( techTotal / numTotal);
                key.setTheroyAvg( theryTotal / numTotal);
                //输出结果
                context.write(key, NullWritable.get());





            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(AvgDriver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(UserScoreBean.class);
                    job.setOutputValueClass(NullWritable.class);

                    job.setMapOutputKeyClass(UserScoreBean.class);
                    job.setMapOutputValueClass(NullWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper.class);
                    job.setReducerClass(FileReduce.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week33\\input"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week33\\oufgfdfgdgh4gf5tput"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }
}
