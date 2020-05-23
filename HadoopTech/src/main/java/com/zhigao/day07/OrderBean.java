package com.zhigao.day07;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String oid;//订单号
    private String uid;//品牌号

    private int num;//商品个数
    private String name;//品牌名称


    public OrderBean() {
    }


    @Override
    public int compareTo(OrderBean o) {
        //pid ==
        int i = o.getUid().compareTo(this.uid);
        if(i == 0)
        {
            return  o.name.compareTo(this.name) ;
        }
        else {
            return  i ;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {

        //
        out.writeUTF(this.uid);
        out.writeUTF(this.oid);
        out.writeUTF(this.name);
        out.writeInt(this.num);



    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid=in.readUTF();
        this.oid=in.readUTF();
        this.name=in.readUTF();
        this.num=in.readInt();


    }
    //所有的get set

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }



//    @Override
//    public String toString() {
//        return this.oid + "\t" + this.name + "\t" + this.num;
//    }

//
    @Override
    public String toString() {
        return "OrderBean{" +
                "oid='" + oid + '\'' +
                ", uid='" + uid + '\'' +
                ", num=" + num +
                ", name='" + name + '\'' +
                '}';
    }
}
