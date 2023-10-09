package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
public class MergeFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.133.200:9000");
        Path inputDirPath = new Path("/input");
        Path outputFilePath = new Path("/output/merge_file.txt");
        init(conf, inputDirPath, outputFilePath, 5, 5);
        doMerge(conf, inputDirPath, outputFilePath);
        HdfsOperator.readFile(conf, "/output/merge_file.txt");
    }

    public static void doMerge(Configuration conf, Path inputDirPath, Path outputFilePath) throws IOException {

    }
    public static void init(Configuration conf, Path inputDirPath, Path outputFilePath,Integer txtNum, Integer shNum){

    }


}