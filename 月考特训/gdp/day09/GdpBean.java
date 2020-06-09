package com.zhigao.day09;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GdpBean implements WritableComparable<GdpBean> {
    private String province;
    private long oneSensor;
    private long twoSensor;
    private long threeSensor;
    private long fourSensor;
    private long sumSensor;

    public GdpBean() {
    }

    public void setGdpBean(String province, long oneSensor, long twoSensor, long threeSensor, long fourSensor) {
        this.province = province;
        this.oneSensor = oneSensor;
        this.twoSensor = twoSensor;
        this.threeSensor = threeSensor;
        this.fourSensor = fourSensor;
        this.sumSensor=this.oneSensor+this.twoSensor+this.threeSensor+this.fourSensor;
    }

    @Override
    public int compareTo(GdpBean o) {
        return o.getProvince().compareTo(this.province);
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.province);
        out.writeLong(this.oneSensor);
        out.writeLong(this.twoSensor);
        out.writeLong(this.threeSensor);
        out.writeLong(this.fourSensor);
        out.writeLong(this.sumSensor);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.province=in.readUTF();
        this.oneSensor=in.readLong();
        this.twoSensor=in.readLong();
        this.threeSensor=in.readLong();
        this.fourSensor=in.readLong();
        this.sumSensor=in.readLong();
    }

    @Override
    public String toString() {
        return province + '\t' +
                sumSensor ;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public long getOneSensor() {
        return oneSensor;
    }

    public void setOneSensor(long oneSensor) {
        this.oneSensor = oneSensor;
    }

    public long getTwoSensor() {
        return twoSensor;
    }

    public void setTwoSensor(long twoSensor) {
        this.twoSensor = twoSensor;
    }

    public long getThreeSensor() {
        return threeSensor;
    }

    public void setThreeSensor(long threeSensor) {
        this.threeSensor = threeSensor;
    }

    public long getFourSensor() {
        return fourSensor;
    }

    public void setFourSensor(long fourSensor) {
        this.fourSensor = fourSensor;
    }

    public long getSumSensor() {
        return sumSensor;
    }

    public void setSumSensor(long sumSensor) {
        this.sumSensor = sumSensor;
    }
}
