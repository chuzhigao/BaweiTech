package com.zhigao.com.zhigao.day05demo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupByCompartor extends WritableComparator {

    public OrderGroupByCompartor() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        int iBrandid = a1.getBrandId().compareTo(b1.getBrandId());
        return iBrandid;
    }
}
