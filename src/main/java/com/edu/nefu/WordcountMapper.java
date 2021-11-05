package com.edu.nefu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

// map阶段
// key in --索引、偏移量
// value in  --String 类型
// key out    --String 类型
// value out
public class WordcountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取一行
        String line = value.toString();
        // 2.切割
        String[] words = line.split("\n");
        // 3.输出

        for (String word : words) {
                k.set(word);
                context.write(k, v);
        }
    }

}
