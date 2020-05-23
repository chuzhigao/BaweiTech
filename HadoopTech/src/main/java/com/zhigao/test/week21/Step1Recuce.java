package com.zhigao.test.week21;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class Step1Recuce extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text cc = new Text() ;

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        HashSet set = new HashSet() ;
        Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext())
        {
            //key 会变化
            iterator.next() ;
            String c  = key.toString() ;
            c.substring(0, c.length() -1 ) ;//.replace('[',' ').replace(']',' ') ;

            System.out.printf(" !!!!!!!!!!!!" + c);
            set.add(c) ;

        }
        Iterator iterator1 = set.iterator();
        while ( iterator1.hasNext()){
            cc.set((String) iterator1.next());
            context.write(cc,NullWritable.get());
        }



    }
}
