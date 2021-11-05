package com.edu.nefu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class moved{
    public void mv(String src,String dst ,FileSystem fs) throws IOException {
        Path srcPath= new Path(src);
        Path dstPath= new Path(dst);
        //指定创建
        if (fs.rename(srcPath,dstPath))
            System.out.println("移动成功");
    }
}
public class move {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();;
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.232.138:9000"), conf,"root");
        //1. 在hdfs上创建路径
        Scanner in = new Scanner(System.in);
        System.out.println("输入源文件地址和目标文件地址");
        String src;
        String dst;
        src = in.nextLine();
        dst = in.nextLine();
        moved a = new moved();
        a.mv(src,dst,fs);
        fs.close();
    }
}
