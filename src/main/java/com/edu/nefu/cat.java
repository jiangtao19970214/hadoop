package com.edu.nefu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class put{
    public void out(String src,FileSystem fs) throws IOException {
        Path srcPath = new Path(src);

        InputStream in = fs.open(srcPath);
        IOUtils.copyBytes(in,System.out,4096,false);
        System.out.println("下载成功！");
    }
}

public class cat {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.232.138:9000");
        FileSystem fs = FileSystem.get(uri, conf,"root");

//      1. 查看文件内容
        Scanner in = new Scanner(System.in);
        System.out.println("输入源文件");
        String src;
        src = in.nextLine();

        put a = new put();
        a.out(src,fs);
        fs.close();
    }
}
