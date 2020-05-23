package com.zhigao.com.zhigao.day05demo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean  implements WritableComparable<OrderBean> {
    //定义字段 订单号，个数。平牌号 品牌名称
    private  String orderId ;
    private  int  orderNum ;
    private  String brandId ;
    private  String brandName ;

    public OrderBean() {

    }

    public void  setOrderBean(String orderId, int orderNum, String brandId, String brandName) {
        this.orderId = orderId;
        this.orderNum = orderNum;
        this.brandId = brandId;
        this.brandName = brandName;
    }

    //实现我们的二次排序
    @Override
    public int compareTo(OrderBean o) {
        int iBrand = o.getBrandId().compareTo(this.brandId);
        return iBrand;
//        if(iBrand==0){
//            return o.getBrandName().compareTo(this.brandName);
//        }
//        else {
//            return  iBrand ;
//        }
    }

    //实现我们的序列化与反序列化
    @Override
    public void write(DataOutput out) throws IOException {

      out.writeUTF( this.orderId);
      out.writeUTF(this.brandId);
      out.writeUTF(this.brandName);
      out.writeInt(this.orderNum);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF() ;
        this.brandId = in.readUTF();
        this.brandName = in.readUTF();
        this.orderNum = in.readInt();

    }
    // tostring

    @Override
    public String toString() {
        return  orderId + '\''
                + brandName +"\t"
                  + orderNum  ;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public int getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(int orderNum) {
        this.orderNum = orderNum;
    }

    public String getBrandId() {
        return brandId;
    }

    public void setBrandId(String brandId) {
        this.brandId = brandId;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }
}
