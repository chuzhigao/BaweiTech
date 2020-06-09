package com.zhigao.day09;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GdpGroupCompaetor extends WritableComparator {
    public GdpGroupCompaetor() {
        super(GdpBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
       GdpBean a1= (GdpBean) a;
       GdpBean b1= (GdpBean) b;
        int i = b1.getProvince().compareTo(a1.getProvince());
        return i;
    }
}
