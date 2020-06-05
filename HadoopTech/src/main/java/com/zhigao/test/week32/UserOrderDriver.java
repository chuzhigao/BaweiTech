package com.zhigao.test.week32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class UserOrderDriver {


        public static class FilmMapper1 extends Mapper<LongWritable,Text,UserOrderBean,NullWritable> {
            Text fileName = new Text() ;
            UserOrderBean userOrderBean = new UserOrderBean();

             

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {

                FileSplit inputSplit = (FileSplit) context.getInputSplit();
                String name = inputSplit.getPath().getName();
                fileName.set(name);
           
            }

            // data => bean 
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String[] split = value.toString().split(",");

//                int length = split.length;
               /**
                 * u001,senge,18,angelababy
                 order001,u001
                 order002,u001
                 */
                  if( split.length == 2){ // 订单表
                   // if(fileName.toString().equals("")){ // 订单表
                      userOrderBean.setOrderId(split[0]);
                      userOrderBean.setUserId( split[1]);
                      userOrderBean.setAge(0);
                      userOrderBean.setUserName("NULL");
                      userOrderBean.setGoodFriend("NULL");
                      context.write(userOrderBean ,NullWritable.get());

                    }
                    else {
                      // u001,senge,18,angelababy
                      userOrderBean.setOrderId("NULL");
                      userOrderBean.setUserId( split[0]);
                      userOrderBean.setAge(Integer.parseInt(split[2]));
                      userOrderBean.setUserName(split[1]);
                      userOrderBean.setGoodFriend(split[3]);
                      context.write(userOrderBean ,NullWritable.get());

                  }


            }
        }

        public static class FileReduce1 extends Reducer<UserOrderBean,NullWritable,UserOrderBean,NullWritable> {

            @Override
            protected void reduce(UserOrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                Iterator<NullWritable> iterator = values.iterator();
                NullWritable next = iterator.next();
                // u001,senge,18,angelababy
                 String username = key.getUserName() ;
                 int age = key.getAge() ;
                 String goodFriend = key.getGoodFriend() ;
                 while (iterator.hasNext()){
                     //挨个loop 将所有的数值给复制到order 输出
                     //订单信息 order002,u001 //补充 u001,senge,18,angelababy
                     iterator.next() ;
                     key.setUserName(username);
                     key.setGoodFriend(goodFriend);
                     key.setAge(age);
                    context.write(key, NullWritable.get());
                 }



            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(UserOrderDriver.class);
                    //特别注意，分组器不加出现问题
                    job.setGroupingComparatorClass(UserGroupCompartor.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(UserOrderBean.class);
                    job.setOutputValueClass(NullWritable.class);

                    job.setMapOutputKeyClass(UserOrderBean.class);
                    job.setMapOutputValueClass(NullWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper1.class);
                    job.setReducerClass(FileReduce1.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week32\\input"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week32\\output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }

}
