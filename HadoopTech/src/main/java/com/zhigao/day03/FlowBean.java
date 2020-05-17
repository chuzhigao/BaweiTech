package com.zhigao.day03;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FlowBean
 * 必须实现的方法
 * 1:读与写的 序列与反序列化，其中顺序必须一致
 * 2:实现空的一个构造函数
 * 3:实现tostring方法
 * 4:
 * 数据格式
 * 序号  手机号      ip             地址             上行流量  下行流量 状态
 * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
 */
public class FlowBean implements Writable {
    private  String telephone ;  //手机号码
    private  long    upFlow ;    //上行流量
    private  long    downFlow ;  // 下行流量
    private  long    sumFlow ;   //总流量

    // 2:实现空的一个构造函数
    public FlowBean() {
    }
    //当设置初始化值的时候自动把总流量计算出来
    public  void setFlowBean(String telephone, long upFlow, long downFlow) {
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
        return "telephone='" + telephone + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow ;
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
}
