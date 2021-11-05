package com.edu.nefu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

class del{
    public void delete(String src,FileSystem fs) throws IOException {
        Path path = new Path(src);
        FileStatus[] file = fs.listStatus(path);
        if (fs.isDirectory(path)) {
            //迭代删除目录
            if (file.length > 0) {
                fs.delete(path, true);
            }
        }
        else if (fs.isFile(path))
        {
            fs.delete(path);
        }
        System.out.println("输入删除成功");
    }
}
public class delet {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();;
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.232.138:9000"), conf,"root");

        Scanner in = new Scanner(System.in);
        System.out.println("输入删除文件地址");
        String src;
        src = in.nextLine();
        del a = new del();
        a.delete(src,fs);
        fs.close();
    }
}
