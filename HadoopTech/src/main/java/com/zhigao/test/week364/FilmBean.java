package com.zhigao.test.week364;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilmBean  implements WritableComparable<FilmBean> {
    // 1:Toy Story (1995):Animation|Childrens|Comedy
    private  String sn ;
    private  String title ;
    private  String year ;
    private  String type ;

    public  void setFilmBean(String sn, String title, String year, String type) {
        this.sn = sn;
        this.title = title;
        this.year = year;
        this.type = type;
    }

    public FilmBean() {
    }

    public String getSn() {
        return sn;
    }

    public void setSn(String sn) {
        this.sn = sn;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int compareTo(FilmBean o) {
        int i = o.getSn().compareTo(this.sn);
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.sn);
        out.writeUTF(this.title);
        out.writeUTF(this.year);
        out.writeUTF(this.type);


    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.sn =  in.readUTF() ;
        this.title =  in.readUTF() ;
        this.year =  in.readUTF() ;
        this.type =  in.readUTF() ;





    }

    @Override
    public String toString() {
        return  sn + '\t' +
                 title + '\t'
                 + year + '\t'
                 + type ;
    }
}
