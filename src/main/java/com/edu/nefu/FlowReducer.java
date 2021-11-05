package com.edu.nefu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    public void reduce(Text key,Iterable<FlowBean> values,Context context) throws IOException, InterruptedException {
        long total_upFlow = 0;
        long total_downFlow = 0;
        for(FlowBean bean:values){
            total_upFlow += bean.getUpFlow();
            total_downFlow += bean.getDownFlow();
        }
        FlowBean v = new FlowBean();
        v.set(total_upFlow,total_downFlow);
        context.write(key,v);
    }
}
