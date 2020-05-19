package com.zhigao.day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <name>fs.defaultFS</name>
 *     <value>hdfs://hadoop101:9000</value>
 * </property>
 * 可以通过core-site.xml 可以知道访问hdfs 中的文件系统中的配置
 * namenode 是在 121 上
 * 通过理论学习我们知道 client =>hdfs =>namenode 然后通过namenode 可以知道
 * 文件的元数据信息，再去datanode上存贮或者读取文件
 */

public class HdfsClient {

    //上传一个文件
    public static void putFile() throws IOException, URISyntaxException, InterruptedException {
        //找到配置=》通过HDFS 系统配置去访问
        Configuration conf = new Configuration() ;
        conf.set("fs.defaultFS","hdfs://192.168.40.121:9000" );
        //通过配置去访问, 其中如果跨平台，需要使用root 用户。
        FileSystem fs = FileSystem.get ( new URI("hdfs://192.168.40.121:9000") , conf,"root") ;

        //上传文件
        fs.copyFromLocalFile( new Path("c:/ccc.sql") , new Path("/chuzhigao/cc1.sql"));
        //
        fs.close();

    }



    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        putFile();
        // 上传
        // testCopyFromLocalFile();
        // 下载
        //CopyFromHdfs();
        //删除文件夹或者文件
        //deleteFold();
        //对文件进行重新命名
        // renameFile();
        //对文章详情了解
        // listFileDetail();
        // 判断文件类型

        //judgeFiles();
    }

    /**
     * 判断为文件类型
     */

    public static void judgeFiles() throws InterruptedException, IOException, URISyntaxException {

        //获取文件系统
        FileSystem fs =  getFs();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus ff:  fileStatuses ) {

            if (ff.isDirectory()){
                System.out.println( "为文件夹" + ff.getPath().getName());
            }
            else {
                System.out.println( "为文件" + ff.getPath().getName());

            }


        }

        fs.close();
    }

    /**
     * 文件的详情
     */

    /**
     *
     */
    public static void listFileDetail() throws InterruptedException, IOException, URISyntaxException {
        //获取hdfs文件系统
        FileSystem fs  =  getFs() ;
        // 获取到文件信息, 第二个参数
        //通过fs的listFiles方法可以自动实现递归(自带递归)列出文件类型，返回的是一个远程可迭代对象,需要传入两个参数，第一个参数是服务器路径，第二个参数是否递归
        RemoteIterator<LocatedFileStatus> filelist = fs.listFiles(new Path("/"), true);
        while (filelist.hasNext()){
            //循环编列所有的文件
            LocatedFileStatus nextFile = filelist.next();
            //获取到文件名
            String name = nextFile.getPath().getName();
            // 获取到大小
            long len = nextFile.getLen();
            //获取到权限
            FsPermission permission = nextFile.getPermission();
            //获取到块的信息
            BlockLocation[] blockLocations = nextFile.getBlockLocations();
            System.out.println(" 打印每个文件信息：" + name + " 大小 =" + len + " 权限 =" +permission );

            for (int i = 0; i < blockLocations.length ; i++) {
                // 获取到每个块坐在机器的域名
                String[] hosts = blockLocations[i].getHosts();
                for (String h: hosts ) {
                    System.out.println("文件名" + name + " 所在快域名为 = " + h);
                }

            }

        }
        //关闭资源
        fs.close();

    }

    public static void renameFile() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs =  getFs();
        fs.rename( new Path("/1.txt"), new Path("/2.txt") );
        fs.close();
    }

    /**
     * 删除文件夹
     */
    public static void deleteFold() throws InterruptedException, IOException, URISyntaxException {

        FileSystem fs  =  getFs() ;
        // 第二个参数，是否递归删除，区别如果是一个文件，无所谓，如果是一个文件夹，下面有文件，设置为FLASE ,不能删除数据
        fs.delete(new Path("/chuzhigao/test") , true) ;
        fs.close();

    }
    /**
     * 拷贝文件到本地
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */

    public static void CopyFromHdfs () throws InterruptedException, IOException, URISyntaxException {

        FileSystem  fs  =  getFs() ;
        //从服务器下载到本地
        fs.copyToLocalFile(false ,new Path("/1.txt") , new Path("c:/chuzhigao/1.txt"));
        fs.close();

    }

    // 文件上传

    public static void testCopyFromLocalFile() throws InterruptedException, IOException, URISyntaxException {
        // 获取文件系统
        FileSystem fs =  getFs();
        fs.copyFromLocalFile(new Path("c:/ccc.sql"), new Path("/chuzhigao/test/chuzhigao.txt") );
        fs.close();
    }

    /**
     *  通过给系统进行抽取，将文件对象获取到
     * @return
     */
    public static FileSystem getFs() throws URISyntaxException, IOException, InterruptedException {
        // 以前的理论，如果对文件的读取或者写入，要与谁联系-》 namenode ->?配置：core-site.xml
        // 中有默认的文件系统 及端口
       /*
       <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop101:9000</value>
        </property>
        */
        // 创立一个配置文件，这样通过配置获取到文件系统，client 通过文件系统访问namenode
        org.apache.hadoop.conf.Configuration cg = new org.apache.hadoop.conf.Configuration();
        cg.set("fs.defaultFS","hdfs://192.168.40.121:9000");
        FileSystem  fileSystem = FileSystem.get( new URI("hdfs://192.168.40.121:9000"), cg,"root") ;
        // 通过文献系统去进行操作
        return  fileSystem ;
    }

    /*
      从以前的理论知道，client -> hdfssysten
     */
    public static void main1(String[] args) throws IOException, URISyntaxException, InterruptedException {

        // 以前的理论，如果对文件的读取或者写入，要与谁联系-》 namenode ->?配置：core-site.xml
        // 中有默认的文件系统 及端口
       /*
       <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop101:9000</value>
        </property>
        */
        // 创立一个配置文件，这样通过配置获取到文件系统，client 通过文件系统访问namenode
        org.apache.hadoop.conf.Configuration cg = new org.apache.hadoop.conf.Configuration();
        cg.set("fs.defaultFS","hdfs://192.168.40.121:9000");
        FileSystem  fileSystem = FileSystem.get( new URI("hdfs://192.168.40.121:9000"), cg,"root") ;
        // 通过文献系统去进行操作
        fileSystem.mkdirs(new Path("/chuzhigao/test")) ;
        fileSystem.close();
    }


}
