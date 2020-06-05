package com.zhigao.test.week32;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserGroupCompartor extends WritableComparator {


    public UserGroupCompartor() {
         super(UserOrderBean.class,true);

    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
         UserOrderBean  a1 = (UserOrderBean) a;

        UserOrderBean  b1 = (UserOrderBean) b;
        return  a1.getUserId().compareTo( b1.getUserId()) ;

    }
}
