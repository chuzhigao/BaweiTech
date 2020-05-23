package com.zhigao.day07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroup extends WritableComparator {
    public OrderGroup() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa= (OrderBean) a;
        OrderBean ob= (OrderBean) b;
        return oa.getUid().compareTo(ob.getUid());
    }

}
