package com.edu.nefu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class load{
    public void out(String src,String dst,FileSystem fs) throws IOException {
        Path srcPath = new Path(src);
        File dstfile = new File(dst);

        FSDataInputStream in = fs.open(srcPath);
        if(!dstfile.exists())
        {
            FileOutputStream out =  new FileOutputStream(dstfile);
            IOUtils.copyBytes(in,out,2048,true);
        }
        else{
            System.out.println("文件已存在存在");
            return;
        }
        System.out.println("下载成功！");

    }
}

public class download {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.232.138:9000");
        FileSystem fs = FileSystem.get(uri, conf,"root");

//      1. 查看文件内容
        Scanner in = new Scanner(System.in);
        System.out.println("输入源文件与目标文件");
        String src,dst;
        src = in.nextLine();
        dst = in.nextLine();
        load a = new load();
        a.out(src,dst,fs);
        fs.close();
    }
}
