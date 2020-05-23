package com.zhigao.day04;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 二次排序的方法：上节课是一次排序，
 * 1:空构造方法，实现序列话与反序列化用
 * 2：读写 的字段的顺序一致
 * 3：比较器规则
 * 4：所有字段的get set 方法
 * 5：toString 方法的实现 重新写
 */

public class OrderBean implements WritableComparable<OrderBean> {
    private  int orderId ;//订单号
    private  double price ;//商品的价格

    //构造方法
    public OrderBean() {
    }

    //赋值

    public void   setOrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    //如果订单号相等，按价格来进行排序，否则按订单号来排序
    public int compareTo(OrderBean o) {

        int compare = Double.compare(o.getPrice(), this.price);
        //先比较订单的顺序
        if(o.getOrderId() == (this.orderId))
        {
            return compare ;
        }
        else
        {
            return  Integer.compare(o.getOrderId(), this.orderId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.orderId);
        out.writeDouble(this.price);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt() ;
        this.price = in.readDouble() ;

    }

    // 所有的get set 方法

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  orderId +
                "\t" + price;
    }
}
