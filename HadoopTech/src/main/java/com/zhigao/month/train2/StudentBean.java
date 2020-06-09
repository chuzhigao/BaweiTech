package com.zhigao.month.train2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentBean  implements WritableComparable<StudentBean>{

    private  String studentId ;// 学生学号
    private  String name ;//学生姓名
    private  String classname ;//科目
    private  int score ;//科目成绩

    public StudentBean() {
    }



    @Override
    public String toString() {
        return "StudentBean{" +
                "studentId='" + studentId + '\'' +
                ", name='" + name + '\'' +
                ", classname='" + classname + '\'' +
                ", score=" + score +
                '}';
    }



    @Override
    public int compareTo(StudentBean o) {

       // 第一次排序按学号排，如果学号相同，按学名排
        return  o.getStudentId().compareTo(this.studentId)==0?o.getName().compareTo(this.name):o.getStudentId().compareTo(this.studentId);

    }

    @Override
    public void write(DataOutput output ) throws IOException {

        output.writeBytes(this.studentId);
        output.writeBytes(this.classname);
        output.writeBytes(this.name);
        output.writeInt(this.score);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.studentId= dataInput.readUTF();
        this.classname = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.score = dataInput.readInt();


    }
//
//    @Override
//    public void readFields(DataInput input) throws IOException {
//
//        this.studentId= input.readUTF();
//        this.classname= input.readUTF();
//        this.name= input.readUTF();
//        this.score= input.readInt();
//
//    }

    public String getStudentId() {
        return studentId;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
