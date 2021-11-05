package com.edu.nefu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class ls1{
    public void putout(String src,FileSystem fs) throws IOException {
        Path path = new Path(src);
        FileStatus [] listStatus = fs.listStatus(path);
        String filename = "";
        for (FileStatus list:listStatus){
            if (list.isFile()){
//                System.out.println(list.getPermission());
                filename = "f";
            }
            else if (list.isDirectory()){
//                String s="/"+list.getPath().getName()+"/";
//                listStatus = fs.listStatus(new Path(s));
//                System.out.println(list.getPath().getName());
                filename = "d";
            }
            else
                filename = "o";
            System.out.println(filename + list.getPermission() + " " + list.getPath().getName());
        }
        System.out.println("输出结束");

    }
}
public class ls_all {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.232.138:9000");
        FileSystem fs = FileSystem.get(uri, conf,"root");

//      1. 查看文件内容
        System.out.println("请输入查看文件");
        Scanner in = new Scanner(System.in);
        String src = in.nextLine();
        ls1 a = new  ls1();
        a.putout(src,fs);
        fs.close();
    }
}
