package com.zhigao.test.week33;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserScoreBean implements WritableComparable<UserScoreBean> {
    private  String useName ; //姓名
    private  int theroyScore; // 理论分数
    private  int techScore;  // 技能分数
    private   int number ;//个数 也是次数
    private  int theroyScoreSum ;//理论总分
    private  int  techScoreSum ;//技能总分

    private  double  theroyAvg ; // 理论平均分
    private  double  techAvg ;// 技能平均分。


    public UserScoreBean() {
    }


    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(useName);
        out.writeInt(theroyScore);
        out.writeInt(techScore);
        out.writeInt(number);
        out.writeInt(theroyScoreSum);
        out.writeInt(techScoreSum);
        out.writeDouble(theroyAvg);
        out.writeDouble(techAvg);



    }

    @Override
    public void readFields(DataInput in ) throws IOException {

        this.useName= in.readUTF();
        this.theroyScore = in.readInt() ;
        this.techScore = in.readInt() ;
        this.number = in.readInt() ;
        this.theroyScoreSum = in.readInt() ;
        this.techScoreSum = in.readInt() ;
        this.theroyAvg = in.readDouble() ;
        this.techAvg = in.readDouble() ;

    }
    // get set 方法

    public void setUserScoreBean(String useName, int theroyScore, int techScore, int number, int theroyScoreSum, int techScoreSum, double theroyAvg, double techAvg) {
        this.useName = useName;
        this.theroyScore = theroyScore;
        this.techScore = techScore;
        this.number = number;
        this.theroyScoreSum = theroyScoreSum;
        this.techScoreSum = techScoreSum;
        this.theroyAvg = theroyAvg;
        this.techAvg = techAvg;
    }

    public String getUseName() {
        return useName;
    }

    public void setUseName(String useName) {
        this.useName = useName;
    }

    public int getTheroyScore() {
        return theroyScore;
    }

    public void setTheroyScore(int theroyScore) {
        this.theroyScore = theroyScore;
    }

    public int getTechScore() {
        return techScore;
    }

    public void setTechScore(int techScore) {
        this.techScore = techScore;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getTheroyScoreSum() {
        return theroyScoreSum;
    }

    public void setTheroyScoreSum(int theroyScoreSum) {
        this.theroyScoreSum = theroyScoreSum;
    }

    public int getTechScoreSum() {
        return techScoreSum;
    }

    public void setTechScoreSum(int techScoreSum) {
        this.techScoreSum = techScoreSum;
    }


    public double getTheroyAvg() {
        return theroyAvg;
    }

    public void setTheroyAvg(double theroyAvg) {
        this.theroyAvg = theroyAvg;
    }

    public double getTechAvg() {
        return techAvg;
    }

    public void setTechAvg(double techAvg) {
        this.techAvg = techAvg;
    }
    //

    @Override
    public String toString() {
        return  useName + '\t' +
                 theroyAvg +'\t'+techAvg;
    }

    @Override
    public int compareTo(UserScoreBean o) {
        int compare = o.getUseName().compareTo(this.useName) ;
        if(compare == 0){

          return  Double.compare( o.getTechAvg() , this.techAvg);
        }
        else
        {
            return compare ;
        }

    }
}
