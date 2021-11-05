package com.edu.nefu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 取一行
        String line = value.toString();
        // 2. 切割
        String[] fields = line.split("\n");
        // 3. 封装
        String num=fields[0];
        long downFlow = Long.parseLong(fields[0]);
        Text v =new Text();
        FlowBean k = new FlowBean();
        k.setDownFlow(downFlow);
        v.set(num);
        // 4. 写出
        context.write(k, v);
    }
}
