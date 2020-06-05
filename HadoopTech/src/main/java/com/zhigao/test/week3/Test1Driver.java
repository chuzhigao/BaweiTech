package com.zhigao.test.week3;
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

public class Test1Driver {


        public static class FilmMapper extends Mapper<LongWritable,Text,StudentBean,NullWritable> {

           StudentBean studentBean = new StudentBean() ;

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// 王五	87  	79  => bean
                String[] split = value.toString().split("\t");
                studentBean.setStudentBean(split[0], Integer.parseInt( split[1]),Integer.parseInt( split[2]), 1,0,0,0.0,0.0 );
               context.write( studentBean , NullWritable.get());
            }
        }

        public static class FileReduce extends Reducer<StudentBean,NullWritable,StudentBean,NullWritable> {

            @Override
            protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int theoryScoreSum =0 ;
                int techScoreSum =0 ;
                int numScore = 0;


                for ( NullWritable c:
                    values ) {
                    theoryScoreSum += key.getTheoryScore() ;
                    techScoreSum += key.getTechScore() ;
                    numScore += key.getTestNum() ;
                }
                double theoryAvg = theoryScoreSum / numScore ;
                double techAvg = techScoreSum / numScore ;
                key.setTechScoreAvg( techAvg);
                key.setTheoryScoreAvg(theoryAvg);

                 context.write(key, NullWritable.get());

            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Test1Driver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(StudentBean.class);
                    job.setOutputValueClass(NullWritable.class);

                    job.setMapOutputKeyClass(StudentBean.class);
                    job.setMapOutputValueClass(NullWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper.class);
                    job.setReducerClass(FileReduce.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week3\\input"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week3\\output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }

}
