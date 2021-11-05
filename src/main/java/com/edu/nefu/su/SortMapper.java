package com.edu.nefu.su;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class SortMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
    private static IntWritable data = new IntWritable();
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        String line = value.toString();
        data.set(Integer.parseInt(line));
        context.write(data, new IntWritable(1));
    }
}
