package com.zhigao.day03;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBeanNew implements WritableComparable<FlowBeanNew> {


    private  String telephone ;  //手机号码
    private  long    upFlow ;    //上行流量
    private  long    downFlow ;  // 下行流量
    private  long    sumFlow ;   //总流量

    //空的构造函数


    public FlowBeanNew() {
    }
    //

    //设置一个新的赋值函数设定总量大小去比较
    public void setBeanNew(String telephone, long upFlow, long downFlow, long sumFlow) {
        this.telephone = telephone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowBeanNew o) {
        int compare = Long.compare(o.getSumFlow(), this.sumFlow);
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.telephone);
        out.writeLong(this.downFlow);
        out.writeLong(this.upFlow);
        out.writeLong(this.sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.telephone=in.readUTF();
        this.downFlow=in.readLong();
        this.upFlow=in.readLong();
        this.sumFlow=in.readLong();

    }

    //必须实现的get set 方法


    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return  telephone + '\t' +  upFlow +
                "\t" + downFlow +
                "\t" + sumFlow ;
    }
}
