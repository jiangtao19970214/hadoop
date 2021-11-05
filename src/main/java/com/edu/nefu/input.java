package com.edu.nefu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class init{
    public void in(String src,String dst,FileSystem fs) throws IOException {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        if(!fs.exists(dstPath))
            fs.copyFromLocalFile(srcPath,dstPath);
        else{
            System.out.println("文件已经存在");
            System.out.println("1、覆盖原文件");
            System.out.println("2、添加到文件末尾");
            System.out.println("0、退出");
            System.out.print("请输入你的选择");
            Scanner in=new Scanner(System.in);
            int x=in.nextInt();
            if(x==1)
            {
                fs.copyFromLocalFile(false,srcPath, dstPath);
            }
            else if(x==2)
                appendFile(src,dst);
            else if(x==0)
                return;
        }
        System.out.println("上传成功！");

        }

    private void appendFile(String src, String dst) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        Path dstPath = new Path(dst);
        InputStream in = new BufferedInputStream(new FileInputStream(src));
        FSDataOutputStream out = fs.append(dstPath);
        IOUtils.copyBytes(in,out,4096,true);
        fs.close();

    }
}

public class input {

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
        init a = new init();
        a.in(src,dst,fs);
        fs.close();
    }
}
