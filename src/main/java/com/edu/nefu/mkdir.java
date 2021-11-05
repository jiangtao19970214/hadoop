package com.edu.nefu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class mk{
    public void make(String src,FileSystem fs) throws IOException {
        Path path = new Path(src);

        //指定创建
        if(!fs.exists(path))
            fs.mkdirs(path);
        else {
            System.out.println("路径已存在");
        }
        System.out.println("创建成功");
    }
}
public class mkdir {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();;
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.232.138:9000"), conf,"root");
        //1. 在hdfs上创建路径
        Scanner in = new Scanner(System.in);
        System.out.println("输入创建文件地址");
        String src;
        src = in.nextLine();

        mk a = new mk();
        a.make(src,fs);
        fs.close();
    }
}
