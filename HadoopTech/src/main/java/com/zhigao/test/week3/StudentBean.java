package com.zhigao.test.week3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentBean  implements WritableComparable<StudentBean> {


    // name   理论 技能
    private  String name ;
    private  int theoryScore ;
    private  int techScore ;


    private  int testNum ;
    private  int theoryScoreSum ;
    private  int techScoreSum ;

    private  double theoryScoreAvg;
    private  double techScoreAvg ;


    public  void setStudentBean(String name, int theoryScore, int techScore, int testNum, int theoryScoreSum, int techScoreSum, double theoryScoreAvg, double techScoreAvg) {
        this.name = name;
        this.theoryScore = theoryScore;
        this.techScore = techScore;
        this.testNum = testNum;
        this.theoryScoreSum = theoryScoreSum;
        this.techScoreSum = techScoreSum;
        this.theoryScoreAvg = theoryScoreAvg;
        this.techScoreAvg = techScoreAvg;
    }

    @Override
    public String toString() {
        return  name +   "\t"+ theoryScoreAvg +
              "\t"+ techScoreAvg ;

    }

    public StudentBean() {
    }



    @Override
    public int compareTo(StudentBean o) {

        int i = o.name.compareTo(this.name);

        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.name);
        out.writeInt(theoryScore);
        out.writeInt(techScore);
        out.writeInt(testNum);
        out.writeInt(theoryScoreSum);
        out.writeInt(techScoreSum);
        out.writeDouble(theoryScoreAvg);
        out.writeDouble(techScoreAvg);


    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF() ;
        this.theoryScore = in.readInt() ;
        this.techScore = in.readInt() ;
        this.testNum = in.readInt() ;
        this.theoryScoreSum = in.readInt() ;
        this.techScoreSum = in.readInt() ;
        this.theoryScoreAvg = in.readDouble() ;
        this.techScoreAvg = in.readDouble() ;

    }
    // get set

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTheoryScore() {
        return theoryScore;
    }

    public void setTheoryScore(int theoryScore) {
        this.theoryScore = theoryScore;
    }

    public int getTechScore() {
        return techScore;
    }

    public void setTechScore(int techScore) {
        this.techScore = techScore;
    }

    public int getTestNum() {
        return testNum;
    }

    public void setTestNum(int testNum) {
        this.testNum = testNum;
    }

    public int getTheoryScoreSum() {
        return theoryScoreSum;
    }

    public void setTheoryScoreSum(int theoryScoreSum) {
        this.theoryScoreSum = theoryScoreSum;
    }

    public int getTechScoreSum() {
        return techScoreSum;
    }

    public void setTechScoreSum(int techScoreSum) {
        this.techScoreSum = techScoreSum;
    }

    public double getTheoryScoreAvg() {
        return theoryScoreAvg;
    }

    public void setTheoryScoreAvg(double theoryScoreAvg) {
        this.theoryScoreAvg = theoryScoreAvg;
    }

    public double getTechScoreAvg() {
        return techScoreAvg;
    }

    public void setTechScoreAvg(double techScoreAvg) {
        this.techScoreAvg = techScoreAvg;
    }
}
