package com.zhigao.month.train2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StudentGroupcompare  extends WritableComparator{


    public StudentGroupcompare() {
        super(StudentBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StudentBean a1 = (StudentBean)a ;
        StudentBean b1 = (StudentBean)b;
        int i = a1.getStudentId()
        .compareTo(b1.getStudentId());

        return i;
    }
}
