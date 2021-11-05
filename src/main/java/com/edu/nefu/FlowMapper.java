package com.edu.nefu;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean > {
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            String Line = value.toString();
            String[] fields = Line.split(" ");
            Text k = new Text();
            FlowBean v = new FlowBean();
            k.set(fields[2]);
            v.setUpFlow(Long.parseLong(fields[2]));
            v.setDownFlow(Long.parseLong(fields[4]));
//            v.setSumFlow(Long.parseLong(fields[fields.length-3])+Long.parseLong(fields[fields.length-2]));
            context.write(k, v);
    }
}
