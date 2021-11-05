package com.edu.nefu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    int sum=0;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 1.累加求和
        for (IntWritable value : values) {
            sum++;
        }
        // 2.输出

        v.set(sum);
        context.write(key,v);
    }

}
