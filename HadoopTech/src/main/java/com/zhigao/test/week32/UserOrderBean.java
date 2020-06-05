package com.zhigao.test.week32;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1: empty contrantor
 * 2: compare to :ordre by
 * 3: serizable unserizalbe
 * 4: to_string
 *
 *
 */

public class UserOrderBean  implements WritableComparable<UserOrderBean> {

    //  用户号 u001, 名字 senge, 年纪 18, 好友 angelababy

    // 订单号 order006, 用户号 u004

    private  String userId ;
    private  String userName ;
    private  int age ;
    private  String goodFriend ;
    private  String orderId ;

    public UserOrderBean() {
    }

    // 组长在前
    @Override
    public int compareTo(UserOrderBean o) {

        // 1：userid 2:username
        int i =  o.getUserId() .compareTo(this.userId);
        if(i == 0){

            return  o.getUserName().compareTo(this.userName) ;
        }
        else {
            return  i ;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /*
            private  String  ;
    private  String  ;
    private  int  ;
    private  String  ;
    private  String  ;


         */
        out.writeUTF(this.userId);
        out.writeUTF(this.userName);
        out.writeInt(this.age);
        out.writeUTF(this.goodFriend);
        out.writeUTF(this.orderId);


    }

    @Override
    public void readFields(DataInput in ) throws IOException {
       this.userId = in.readUTF() ;
        this.userName = in.readUTF() ;
        this.age = in.readInt() ;
        this.goodFriend = in.readUTF() ;
        this.orderId = in.readUTF() ;


    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getGoodFriend() {
        return goodFriend;
    }

    public void setGoodFriend(String goodFriend) {
        this.goodFriend = goodFriend;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }


        @Override
   public String toString() {

        return  userId + '\t' +
                 userName + '\t'
                + age + '\t' +
                 goodFriend + '\t' +
                 orderId ;
    }
}
