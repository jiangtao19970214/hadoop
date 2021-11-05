package com.edu.nefu.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {


    private long downFlow;

    public FlowBean() {
        super();
    }

    public FlowBean(long downFlow) {
        super();
    }

    //比较
    @Override
    public int compareTo(FlowBean o) {
        int res;
        if (this.downFlow > o.getDownFlow()){
            res=1;
        }
        else if(this.downFlow < o.getDownFlow()){
            res = -1;

        }
        else{
            res=0;
        }
            return 0;
    }

    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(downFlow);
    }

    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.downFlow=dataInput.readLong();


    }

    @Override
    public String toString() {
        return "\t"+downFlow;
    }



    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
}
