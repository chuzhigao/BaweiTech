package com.zhigao.day09;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GdpsortCompartor extends WritableComparator {

    public GdpsortCompartor() {
        super(GdpBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GdpBean a1= (GdpBean) a;
        GdpBean b1= (GdpBean) b;
        return  Long.compare(b1.getSumSensor(),a1.getSumSensor()) ;


    }
}
