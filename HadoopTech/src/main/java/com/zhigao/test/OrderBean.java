package com.zhigao.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  * 二次排序的方法：上节课是一次排序，
 * 1:空构造方法，实现序列话与反序列化用
 * 2：读写 的字段的顺序一致
 * 3：比较器规则
 * 4：所有字段的get set 方法
 * 5：toString 方法的实现 重新写
 *
 */

public class OrderBean implements WritableComparable<OrderBean> {

    private  int orderId ;
    private  double price ;





    // contrat fucntion


    public OrderBean() {
    }

    //赋值
    public void setOrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    /**
     * 比较器  select * from table order by c1 desc ,c2 desc ,c3desc
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        // 订单 =》价格
        int compare = Double.compare(this.price,o.getPrice() );
        int compare1 = Integer.compare(o.getOrderId(), this.orderId);
        if(compare1 == 0){

            return  compare ;
        }
        else {
            return  compare1 ;
        }
    }

    //
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.orderId);
        out.writeDouble(this.price);

    }

    @Override
    public void readFields(DataInput in ) throws IOException {

        this.orderId = in.readInt() ;
        this.price = in.readDouble() ;

    }

    // get set

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
                "\t"  + this.price ;
    }
}
