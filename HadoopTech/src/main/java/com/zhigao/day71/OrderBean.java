package com.zhigao.day71;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public int compareTo(OrderBean o) {
       //pid ==
        int i = o.getPid().compareTo(this.pid);
         if(i == 0)
         {
             return  o.pname.compareTo(this.pname) ;
         }
         else {
             return  i ;
         }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeUTF(pname);
        out.writeInt(amount);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.pname = in.readUTF();
        this.amount = in.readInt();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }


}
