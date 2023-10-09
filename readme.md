## HdfsOperator 类介绍
### 该类将java操作hdfs的相关操作封装成了类方法，如下：
- `createFile` 创建文件
- `createDirectory` 创建目录
- `deleteFile` 删除文件
- `deleteDirectory` 删除目录
- `uploadFile` 从本地上传文件到HDFS
- `readFile` 打印文件内容
- `listFile` 列出某目录下的所有文件信息
- `appendToFile` 向某HDFS文件中追加信息

运行main方法，查看这些方法的使用案例。
注意代码中： 
```
Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://192.168.133.200:9000");
```
fs.defaultFS设置为hdfs://ip:hdfs端口号(不同人的端口号不同,详细请参考在线文档)

## Exercise 

### 实现MergeFile类中的init和doMerge方法

```
init(Configuration conf, Path inputDirPath, Path outputFilePath,Integer txtNum, Integer shNum){}
```

该方法实现以下功能：
- 如果`inputDirPath`目录存在则删除该目录下的所有信息，如果目录不存在则创建该目录。
- 创建`outputFilePat`h文件，如果`outputFileName`文件存在则先删除该文件再创建。
- 在`inputDirPath`目录下创建相关文件，输入和输入如下所示：
  - input: txtNum=3, shNum=2
  - output: file1.txt, file2.txt, file3.txt, file4.sh, file3.sh

```
doMerge(Configuration conf, Path inputDirPath, Path outputFilePath)
```

该方法实现以下功能：
- 筛选出inputDirPath中后缀为txt的文件，并将文件名汇总输入到outputFilePath文件中。
- 比如：inputDirPath目录下有3个文件，file1.txt, file2.txt, file3.sh，则运行doMerge方法后，outputFilePath中的内容为:
```
file1.txt
file2.txt
```