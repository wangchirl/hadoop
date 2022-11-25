package com.shadow.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

public class HdfsClient {
    public static void main(String[] args) throws Exception {

        // 1、获取文件系统
        Configuration conf = new Configuration();
        // FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf);
        // 设置分区数量
        conf.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "root");


        // 2、创建目录
        fileSystem.mkdirs(new Path("/sanguo"));

        // 2.1 文件上传
        fileSystem.copyFromLocalFile(new Path("G:\\bigdata\\hadoop\\datas\\hdfs\\shuguo.txt"), new Path("/sanguo/shuguo.txt"));
        fileSystem.copyFromLocalFile(new Path("G:\\bigdata\\hadoop\\datas\\hdfs\\wuguo.txt"), new Path("/sanguo/wuguo.txt"));

        // 2.2 文件下载
        fileSystem.copyToLocalFile(false, new Path("/sanguo/shuguo.txt"), new Path("./shuguo.txt"));

        // 2.3 文件更名及移动
        fileSystem.rename(new Path("/sanguo/shuguo.txt"), new Path("/sanguo/shuguo2.txt"));

        // 2.4 删除文件
        fileSystem.delete(new Path("/sanguo/wuguo.txt"), true);

        // 2.5 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus locatedFileStatus = listFiles.next();
            System.out.println("================" + locatedFileStatus.getPath() + "======================");
            System.out.println(locatedFileStatus.getPermission());
            System.out.println(locatedFileStatus.getOwner());
            System.out.println(locatedFileStatus.getGroup());
            System.out.println(locatedFileStatus.getLen());
            System.out.println(locatedFileStatus.getModificationTime());
            System.out.println(locatedFileStatus.getReplication());
            System.out.println(locatedFileStatus.getBlockSize());
            System.out.println(locatedFileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

        // 2.6 判断文件和文件夹
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("file:" + fileStatus.getPath().getName());
            } else {
                System.out.println("dictionary:" + fileStatus.getPath().getName());
            }
        }


        // 3、关闭资源
        fileSystem.close();

    }
}
