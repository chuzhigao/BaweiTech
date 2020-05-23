package com.zhigao.telepflow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TeleptoneBean implements WritableComparable<TeleptoneBean> {
    private  String telephone ;  //手机号码
    private  long    upFlow ;    //上行流量
    private  long    downFlow ;  // 下行流量
    private  long    sumFlow ;   //总流量

    // 2:实现空的一个构造函数
    public TeleptoneBean() {
    }
    //当设置初始化值的时候自动把总流量计算出来
    public  void setTeleptoneBean(String telephone, long upFlow, long downFlow) {
        this.telephone = telephone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow + this.downFlow ;
    }

    // 写出
    @Override
    public void write(DataOutput out ) throws IOException {
        out.writeUTF( this.telephone);
        out.writeLong( this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);

    }

    // 1:读与写的 序列与反序列化，其中顺序必须一致
    @Override
    public void readFields(DataInput in ) throws IOException {
        this.telephone = in.readUTF() ;
        this.upFlow = in.readLong() ;
        this.downFlow = in.readLong() ;
        this.sumFlow = in.readLong() ;

    }

    // 3:实现tostring方法

    @Override
    public String toString() {
        return telephone + '\t' +
                + upFlow
                + '\t' + downFlow
                + '\t' + sumFlow ;
    }


    //所有的get set 方法


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
    public int compareTo(TeleptoneBean o) {
        int compare = Long.compare(o.getSumFlow(), this.sumFlow);
        return  compare;
    }


}
