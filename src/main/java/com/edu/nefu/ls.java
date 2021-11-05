package com.edu.nefu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ls {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.232.138:9000");
        FileSystem fs = FileSystem.get(uri, conf,"root");

//      1. 查看文件内容

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //递归列出该目录下所有文件，不包括文件夹，后面的布尔值为是否递归
        while(listFiles.hasNext()) {//如果listfiles里还有东西
            LocatedFileStatus next = listFiles.next();//得到下一个并pop出listFiles
            System.out.println(next.getPath().getName());//输出
        }

        fs.close();
    }
}
