package com.edu.nefu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //
//        int i=0;
//        for (Text value:values){
//            i++;
//            String s;
//            s=String.valueOf(i);
//            value.set(s);
//            context.write(value,key);
//        }
        for(Text value:values){
            context.write(value,key);
        }
    }
}
