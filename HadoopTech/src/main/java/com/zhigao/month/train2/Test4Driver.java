package com.zhigao.month.train2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Test4Driver {


        public static class FilmMapper extends Mapper<LongWritable,Text,StudentBean,NullWritable> {
            StudentBean studentBean= new StudentBean();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] c =   value.toString().split(",");
                if(c.length== 2 ){
                    studentBean.setStudentId(c[0]);
                    studentBean.setName(c[1]);
                    studentBean.setClassname("");
                    studentBean.setScore(0);
                    context.write(studentBean,NullWritable.get());

                }
                if(c.length== 3){
                    studentBean.setStudentId(c[0]);
                    studentBean.setName("");
                    studentBean.setClassname(c[1]);
                    studentBean.setScore(Integer.parseInt(c[2]));

                    context.write(studentBean,NullWritable.get());
                }
            }

            /*
              value => bean
              A001	shangsan
              A001	shangsan
              A001	math	80

              String studentId, String name, String classname, int score
            */

            protected void map1(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String[] c =   value.toString().split(",");
                if(c.length== 2 ){
                    studentBean.setStudentId(c[0]);
                    studentBean.setName(c[1]);
                    studentBean.setClassname("");
                    studentBean.setScore(0);
                    context.write(studentBean,NullWritable.get());

                }
                else{
                    studentBean.setStudentId(c[0]);
                    studentBean.setName("");
                    studentBean.setClassname(c[1]);
                    studentBean.setScore(Integer.parseInt(c[2]));

                    context.write(studentBean,NullWritable.get());
                }


            }
        }

        public static class FileReduce extends Reducer<StudentBean,NullWritable,StudentBean,NullWritable>  {



           // StudentBean cc = new StudentBean();

            @Override
            protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                // STEP 1 取学生表
                Iterator<NullWritable> c = values.iterator();
                c.next();
                String name = key.getName();

                //  将所有的分数表中国的学生名字替换
                while (c.hasNext()){
                    c.next();
                    //这里获取到的就是学生成绩表
                    String valuecj = key.getClassname() + "=" + key.getScore();
                    key.setName(name);
                   context.write(key
                           , NullWritable.get());

                }
            }


        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Test4Driver.class);
                    // SET THE GOURP COMPARE
                    job.setGroupingComparatorClass(StudentGroupcompare.class);
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
                  FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\month04"));
      //      FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day07\\input"));


                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\workspace\\BaweiTech\\HadoopTech\\src\\main\\resources\\monthddgdgffsfdsfsffd0wgdfgfds1sfdgd4qqdd1output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }

}
