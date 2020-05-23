package com.zhigao.day08;

import com.zhigao.day01.WordCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.io.IOException;

/**
 * 程序分析过程
 * 1：讲每个单词再那个文件出现次数给求出
 * 2：讲所有的单词按文件出现次数给汇总
 *     以前的reduce 过程是求合计，现在是求单词合并
 *
 *
 *
 */


public class WordDistrubterDriver {

    public static class  WordDistrubterReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        IntWritable sumtotal = new IntWritable(0);
        // 根据给定的单词进行数据的汇总


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumTotalnow = 0 ;
            for (IntWritable c:
                 values ) {
                sumTotalnow += c.get() ;
            }
            sumtotal.set(sumTotalnow);
            //输出结果
            context.write(key,sumtotal);


        }
    }

    public static class WordDistrubterMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        //建立一个保存文件的Text
        Text fileName = new Text() ;
        //保存文件中切分出来的额单词
        Text wordKey = new Text() ;
        IntWritable num = new IntWritable(1) ;

        /**
         * 所有的mapreduce  都有setup 表示在做mapreduce 过程中只要进程启动，必须先立条件，可以保存一些
         * 常用的数据在context上下文，进行预处理
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取文件的切片，可以通过切片找到所有文件块的信息比如文件名 路径等
            
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            fileName.set(name);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 讲所有文件的切片与文件名进行结合
            // atguigu pingping
            String[] split = value.toString().split(" ");
            for (String word:
                 split) {
                String wordnew = word+"--"+fileName.toString() ;
                wordKey.set(wordnew);
                context.write(wordKey, num);
            }


        }
    }

    /**
     * atguigu--a.txt	3
     atguigu--b.txt	2
      aigui a.txt=>3  b.txt=>2
      可以通过切分 成为 key 单词 后满是次数 ：【（atigui, a.txt=>3）】
      reduce 去对数据进行追加
     */
    public static class  WordDistrubter2Mapper extends  Mapper<LongWritable,Text,Text,Text> {
        Text keyNew = new Text() ;
        Text valuNew = new Text() ;


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("--");
            keyNew.set(split[0]);
            String[] split1 = split[1].split("\t");
            valuNew.set(split1[0]+"=>" + split1[1]  );

            context.write(keyNew, valuNew);

        }

    }

    public static class  WordDistrubter2Reduce extends Reducer<Text,Text,Text,Text > {

        Text outValue  = new Text() ;

        StringBuffer buffer = new StringBuffer() ;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for ( Text c:
                 values) {
                buffer.append(c).append("\t") ;

            }
            outValue.set(buffer.toString());
            context.write( key, outValue);
        }
    }

    //主要过程设置
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration  cfg = new Configuration() ;
                //获取到任务
                Job job = Job.getInstance(cfg) ;
                job.setJarByClass(WordDistrubterDriver.class);
                //对输入输出参数设置
                // Text, IntWritable

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                //设置map reduce类
                job.setMapperClass(WordDistrubterMapper.class);
                job.setReducerClass(WordDistrubterReduce.class);

                //设置输入输出路径
                FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\day08\\input"));
                FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\day08\\output"));
                boolean b = job.waitForCompletion(true);
                // 更具上一个的结果进行下个的重新再一次计算

                if(b==true){
                            //获取到任务
                    Job job2 = Job.getInstance(cfg) ;
                    job2.setJarByClass(WordDistrubterDriver.class);
                    //对输入输出参数设置
                    // Text, IntWritable
                    job2.setOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);

                    job2.setMapOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);
                    //设置map reduce类
                    job2.setMapperClass(WordDistrubter2Mapper.class);
                    job2.setReducerClass(WordDistrubter2Reduce.class);

                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job2,new Path("C:\\HadoopTeacher\\data\\day08\\output"));
                    FileOutputFormat.setOutputPath(job2, new Path("C:\\HadoopTeacher\\data\\day08\\output1"));
                    boolean b2 = job2.waitForCompletion(true);
                    System.exit( b2== true?0:-1);


                }
    }





}
