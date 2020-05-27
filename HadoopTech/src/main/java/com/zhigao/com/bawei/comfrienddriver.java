package com.zhigao.com.bawei;

import com.zhigao.day01.WordCountJob;
import org.apache.hadoop.io.LongWritable;
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
import java.util.ArrayList;
import java.util.Collections;

/**
 * 算法过程
 1
 E:B,C,D,M,L
 进行穷举
 找到人：找到这个人的朋友
 a: : => 人：盆友 (E, B,C,D,M,L）

 b : E, B,C,D,M,L
 穷举
 E-B
 E-C
 E-D
 E-M
 E-L
 ..

 FOR(I =0 ; I< LENT -1 ; I++ ){
 FOR(J= I+1 ; I<LENT; J++)
 {
 排列组合 i->j

 }
 }

 2:
 map->reduce
 B-C    E
 B-C    f

 a:map ,
 b: reduce :
 b-c  : e ,f  由2行多行=>STRINGBUFFER => 行




 人  朋友
 E   B C D M L
 先找到他朋友22组合可能行
 E : 穷举   人
 B-C    E
 b-l    E
 c-l    E
 d-l    E
 m-l    E

 */

public class comfrienddriver {

    //mapreduce 过程


    //stop 1 搭架子
    public static class  Comfriend1Mapper extends Mapper<LongWritable,Text, Text,Text> {

        Text user = new Text() ;//人
        Text fdValue = new Text() ;
        /**
         *  1
         E:B,C,D,M,L
         进行穷举
         找到人：找到这个人的朋友
         a: : => 人：盆友 (E, B,C,D,M,L）

         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String person = split[0] ;
            String[] friend = split[1].split(",");
            user.set(person);
            for (String fd:
                 friend ) {

                fdValue.set(fd); //所有这个人的朋友
                context.write( fdValue, user);
            }

        }
    }

    /**
     * .

     FOR(I =0 ; I< LENT -1 ; I++ ){
     FOR(J= I+1 ; I<LENT; J++)
     {
     排列组合 i->j

     }
     }
     人：盆友 (E, B,C,D,M,L）
     */

    public static class  Comfriend1Reduce  extends Reducer<Text,Text, Text,Text> {
        Text  personKey = new Text() ;

        @Override
        protected void reduce(Text friend, Iterable<Text> user, Context context) throws IOException, InterruptedException {
           //保存这个人所有的可能朋友
            ArrayList<String > userList = new ArrayList<String>();

            for (Text c:
                user ) {
                userList.add(c.toString()) ;
            }
            //进行按hashcode  值进行排序，排除掉 a-b  b-a 是不同的key .
            Collections.sort(userList);
            for (int i = 0; i < userList.size() -1  ; i++) {

                for (int j = i+1; j < userList.size() ; j++) {
                    // 对于一个人的好友，我们进行22 匹配得到所有可能的结果。
                    String person2person = userList.get(i) + "->" + userList.get(j) ;
                    personKey.set(person2person);
                    context.write(personKey, friend) ;


                }

            }




        }
    }

    /**
     *  map->reduce
     B-C    E
     B-C    f

     a:map ,
     b: reduce :
     b-c  : e ,f  由2行多行=>STRINGBUFFER => 行

     */
    //    B-C    E
    public static class  Comfriend2Mapper extends Mapper<LongWritable,Text, Text,Text> {
        Text keyT = new Text() ;
        Text valueT = new Text() ;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            keyT.set(split[0]);
            valueT.set(split[1]);
          context.write( keyT, valueT);

        }
    }

    public static class  Comfriend2Reduce  extends Reducer<Text,Text, Text,Text> {
        Text valueT = new Text() ;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           StringBuffer buffer = new StringBuffer() ;
            for (Text c:
                 values) {
                buffer.append(c.toString()).append("\t") ;
            }
            valueT.set(buffer.toString());
            context.write(key, valueT);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //两个过程
        //mapreduce => e-f : a

        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(comfrienddriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                //设置map reduce类
                job.setMapperClass(Comfriend1Mapper.class);
                job.setReducerClass(Comfriend1Reduce.class);

                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\data10\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\data10\\input1"));
                boolean b = job.waitForCompletion(true);
                if(b==true){

                    //获取到任务
                    Job job2 = Job.getInstance(cfg) ;
                    job2.setJarByClass(comfrienddriver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job2.setOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);

                    job2.setMapOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);
                    //设置map reduce类
                    job2.setMapperClass(Comfriend2Mapper.class);
                    job2.setReducerClass(Comfriend2Reduce.class);

                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job2,new Path("C:\\HadoopTeacher\\data\\data10\\input1"));
                    FileOutputFormat.setOutputPath(job2, new Path("C:\\HadoopTeacher\\data\\data10\\input2"));
                    boolean b1 = job2.waitForCompletion(true);
                    System.exit( b1== true?0:-1);


                }


        // e-f : a  e=f :b => e-f : a, b  ...

    }


}
