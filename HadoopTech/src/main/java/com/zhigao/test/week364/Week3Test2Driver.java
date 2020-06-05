package com.zhigao.test.week364;

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
public class Week3Test2Driver {


        public static class FilmMapper extends Mapper<LongWritable,Text,FilmBean,NullWritable> {
            FilmBean filmBean = new FilmBean();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               //1:Toy Story (1995):Animation|Childrens|Comedy
                String[] split = value.toString().split(":");

                if(split.length == 3){

                    String year = split[1].substring( split[1].length()-5 ,split[1].length() -1 ) ;


                    String[] split1 = split[2].split("\\|");
                    for ( String c :
                            split1 ) {
                        //按照切割完的电影类型给赋值，循环 一条记录变多条
                        filmBean.setFilmBean(split[0], split[1], year , c );
                        context.write(filmBean , NullWritable.get());
                    }

                }



            }
        }

        public static class FileReduce extends Reducer<FilmBean,NullWritable,FilmBean,NullWritable> {

            @Override
            protected void reduce(FilmBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                context.write( key, NullWritable.get());
            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration  cfg = new Configuration() ;
                    //获取到任务
                    Job job = Job.getInstance(cfg) ;
                    job.setJarByClass(Week3Test2Driver.class);
                    //对输入输出参数设置
                    // Text, IntWritable

                    job.setOutputKeyClass(FilmBean.class);
                    job.setOutputValueClass(NullWritable.class);

                    job.setMapOutputKeyClass(FilmBean.class);
                    job.setMapOutputValueClass(NullWritable.class);
                    //设置map reduce类
                    job.setMapperClass(FilmMapper.class);
                    job.setReducerClass(FileReduce.class);




                    //设置输入输出路径
                    FileInputFormat.setInputPaths(job,new Path("C:\\HadoopTeacher\\data\\test\\week364\\input"));
                    FileOutputFormat.setOutputPath(job, new Path("C:\\HadoopTeacher\\data\\test\\week364\\output"));
                    boolean b = job.waitForCompletion(true);
                    System.exit( b== true?0:-1);



        }

}
