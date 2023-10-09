package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class HdfsOperator {
    public static void main(String[] args) {
        // Set Hadoop configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.133.200:9000");

        try {
            // Create a new directory in HDFS
            createDirectory(conf, "/example");

            // Upload a file to HDFS
            uploadFile(conf, "data/test.txt", "/example/test.txt");

            // Read a file from HDFS
            System.out.println("\nthe content of /example/test.txt is :");
            readFile(conf, "/example/test.txt");

            // 判断 /example/test.txt 是否存在
            if(existsPath(conf, "/example/test.txt")) System.out.println("\n/example/test.txt exists");
            else System.out.println("\n/example/test.txt does not  exist");

            // 判断 /example/test1.txt 是否存在
            if(existsPath(conf, "/example/test1.txt")) System.out.println("/example/test1.txt exists\n");
            else System.out.println("/example/test1.txt does not exist\n");

            // Create a file in HDFS
            createFile(conf, "/example/test1.txt");

            // Append "Hello World!" to this file
            appendToFile(conf, "/example/test1.txt", "Hello World!");

            // Read a file from HDFS
            System.out.println("\nthe content of /example/test1.txt is :");
            readFile(conf, "/example/test1.txt");


            // list all files in /example
            System.out.println("\nall files in /example are :");
            List<String> files1 = listFile(conf, "/example");
            for (String file : files1) {
                System.out.println(file);
            }

            // delete /example/test1.txt
            System.out.println("\ndelete /example/test1.txt");
            deleteFile(conf, "/example/test1.txt");

            // list all files in /example again
            System.out.println("\nall files in /example are :");
            List<String> files2 = listFile(conf, "/example");
            for (String file : files2) {
                System.out.println(file);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createDirectory(Configuration conf, String directoryPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path directory = new Path(directoryPath);
        if (!fs.exists(directory)) {
            fs.mkdirs(directory);
            System.out.println("Directory created: " + directoryPath);
        } else {
            System.out.println("Directory already exists: " + directoryPath);
        }
        fs.close();
    }

    public static void createFile(Configuration conf, String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(filePath);
        if (!fs.exists(file)) {
            fs.create(file);
            System.out.println("File created: " + filePath);
        } else {
            System.out.println("File already exists: " + filePath);
        }
        fs.close();
    }

    public static void deleteFile(Configuration conf, String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(filePath);

        if (!fs.exists(file)) {
            System.out.println("File does not exist: " + filePath);
            return;
        }

        boolean deleted = fs.delete(file, false);
        if (deleted) {
            System.out.println("File deleted successfully: " + filePath);
        } else {
            System.out.println("Failed to delete file: " + filePath);
        }

        fs.close();
    }

    public static void deleteDirectory(Configuration conf, String directoryPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path directory = new Path(directoryPath);

        if (!fs.exists(directory)) {
            System.out.println("Directory does not exist: " + directoryPath);
            return;
        }

        boolean deleted = fs.delete(directory, true);
        if (deleted) {
            System.out.println("Directory deleted successfully: " + directoryPath);
        } else {
            System.out.println("Failed to delete directory: " + directoryPath);
        }

        fs.close();
    }

    public static boolean existsPath(Configuration conf, String pathName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathName);
        return fs.exists(path);
    }

    public static void uploadFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path localFile = new Path(localFilePath);
        Path remoteFile = new Path(remoteFilePath);
        fs.copyFromLocalFile(localFile, remoteFile);
        System.out.println("File uploaded to HDFS: " + remoteFilePath);
        fs.close();
    }

    public static void readFile(Configuration conf, String filePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(filePath);
        InputStream in = fs.open(file);
        byte[] buffer = new byte[1024];
        int bytesRead = in.read(buffer);
        while (bytesRead > 0) {
            System.out.write(buffer, 0, bytesRead);
            bytesRead = in.read(buffer);
        }
        in.close();
        fs.close();
    }

    public static List<String> listFile(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path dirName = new Path(dirPath);
        List<String> fileList = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fs.listStatus(dirName);
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    fileList.add(fileStatus.getPath().getName());
                }
            }
        }catch (IOException e){
            System.out.println("Directory not exists:" + e.getMessage());
        }


        fs.close();
        return fileList;
    }

    public static void appendToFile(Configuration conf, String filePath, String content) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(filePath);

        if (!fs.exists(file)) {
            System.out.println("File does not exist: " + filePath);
            return;
        }

        try (OutputStream outputStream = fs.append(file);
             OutputStreamWriter writer = new OutputStreamWriter(outputStream)) {
            writer.write(content);
            writer.write("\n");
            writer.flush();
        }

        System.out.println("Content appended to file successfully.");
        fs.close();
    }

}
