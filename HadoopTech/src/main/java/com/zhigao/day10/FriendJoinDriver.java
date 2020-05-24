package com.zhigao.day10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A:B,C,D,F,E,O
 E:B,C,D,M,L\

 b->a
 ...

 b c d a->e


 a->e  b c d

 b->c a
 b-d  a
 b-f
 b-e
 b-0
 c-d
 c-f
 c-e
 c-0
 ...
 e-0   a

 b-c e
 b-d e
 b-m e
 b-l e

 1: A: B,C,D,F,E,O
  中间人：所有朋友
 所有朋友 与中间人配对
 把可能的2个2个组合成可能的共同关系:共同人

 2：将共同关系的KEY 进行聚合找到可能所有的共同好友

 */

public class FriendJoinDriver {
    /**
     * A:B,C,D,F,E,O
     * =》B:A C=>A
     */
    public static class FriendJoin1Mapper extends Mapper<LongWritable,Text,Text,Text> {
       Text keyNew=new Text();//保存朋友
       Text valNew=new Text();//保存共同人

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            valNew.set(split[0]);
            String[] split1 = split[1].split(",");//朋友
            for (String c:
                 split1) {
                keyNew.set(c);
                context.write(keyNew,valNew);

            }

        }
    }
    //A:B,C,D,F,E,O

    /**
     *  B A
     *  C A
     *  D A
     *  F A
     *  E A
     *  0 A
     *
     *  =>reduce后
     *
     */
    public static class FriendJoin1Reduce extends Reducer<Text,Text,Text,Text>{
        //A:B,C,D,F,E,O这里会将所有共同人A的所有朋友给得出

        Text keyNew=new Text();
        @Override
        protected void reduce(Text friend, Iterable<Text> user, Context context) throws IOException, InterruptedException {
            ArrayList userList=new ArrayList();
            for (Text c:
                 user) {
                userList.add(c.toString());
            }
            //这些用户的顺序进行排序
            Collections.sort(userList);
            //根据得到的规则将所有可能的22排序以及共同基友输出
            for (int i = 0; i < userList.size() -1; i++) {
                for (int j = i+1; j <userList.size() ; j++) {
                    String keyP=userList.get(i)+"->"+userList.get(j);
                    keyNew.set(keyP);
                    context.write(keyNew,friend);
                }
            }


        }
    }
    //B-C	A
    public static class FriendJoin2Mapper extends  Mapper<LongWritable,Text,Text,Text>{

        Text keyNEW=new Text();
        Text valueNEW=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            keyNEW.set(split[0]);
            valueNEW.set(split[1]);
            context.write(keyNEW,valueNEW);

        }
    }
    public static class FriendJoin2Reduce extends Reducer<Text,Text,Text,Text>{

        Text outValue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer buffer=new StringBuffer();
            for (Text c:
                values ) {
                buffer.append(c.toString()).append("\t");
            }
            outValue.set(buffer.toString());
            context.write(key,outValue);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(FriendJoinDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                //设置map reduce类
                job.setMapperClass(FriendJoin1Mapper.class);
                job.setReducerClass(FriendJoin1Reduce.class);




                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\data10\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\data10\\input1"));
                boolean b = job.waitForCompletion(true);
               if(b==true){


                   Job job1 = Job.getInstance(cfg) ;
                   job1.setJarByClass(FriendJoinDriver.class);
                   //对输入输出参数设置
                   // Text, IntWritable

                   job1.setOutputKeyClass(Text.class);
                   job1.setOutputValueClass(Text.class);

                   job1.setMapOutputKeyClass(Text.class);
                   job1.setMapOutputValueClass(Text.class);
                   //设置map reduce类
                   job1.setMapperClass(FriendJoin2Mapper.class);
                   job1.setReducerClass(FriendJoin2Reduce.class);




                   //设置输入输出路径
                   FileInputFormat.setInputPaths(job1,new Path("C:\\HadoopTeacher\\data\\data10\\input1"));
                   FileOutputFormat.setOutputPath(job1, new Path("C:\\HadoopTeacher\\data\\data10\\input11"));
                   boolean b1 = job1.waitForCompletion(true);
                   System.exit( b1== true?0:-1);
               }



    }
}
