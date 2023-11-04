# 1. 作业5说明

**211275009 陈铭浩**

---

## 1.1. maven创建项目后，更改pom.xml相关配置
```
#使用maven-archetype-quickstart(version=1.4)
1. 修改编译使用的jdk版本（本机为JDK1.8）
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
  </properties>
2. 添加项目用到的Java依赖包
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.11</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>3.3.6</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>3.3.6</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs</artifactId>
      <version>3.3.6</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-core</artifactId>
      <version>3.3.6</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-common</artifactId>
      <version>3.3.6</version>
    </dependency>
  </dependencies>
```

---

## 1.2. 程序设计思路
1. 实验要求
对于给定的两个输⼊⽂件A和B（如附件所示），⽂件内包含了纳斯达克100指数、标普500指
数、道琼斯⼯业指数的部分成分股数据，第⼀列为三⼤指数编号（简化为101、102、103），第
⼆列为成分股代码。请编写Mapreduce程序，对两个⽂件内容进⾏合并，并对重复内容进⾏剔
除，最后合并为⼀个包含三⼤指数成分股的输出⽂件。输出⽂件格式与⽂件A和B格式相同。
2. 设计思路
首先将xlsx文件转为csv文件便于后续处理，而后在UnionCalc主类中包括UnionCalcMapper 类，UnionCalcReducer 类和main方法
- UnionCalcMapper 类：
  这是一个Mapper类，用于处理输入数据。
  在 map 方法中，将每一行文本数据（CSV记录）转换为字符串，然后使用逗号分隔符拆分记录，提取索引和股票代码。
  为了去除不必要的空格，使用 .trim() 方法来修整索引和股票代码。
  最后，将索引作为键，股票代码作为值，发送到Reducer以分组相同索引的记录。

- UnionCalcReducer 类：
  这是一个Reducer类，用于处理来自Mapper的键值对数据，去除重复记录。
  在 reduce 方法中，创建了一个 Set 集合（stockCodeSet）来存储唯一的股票代码。
  然后，遍历相同索引下的所有股票代码，将它们添加到 stockCodeSet 中，从而去除重复。
  最后，将去除重复后的记录输出。

- main 方法：
  main 方法是程序的入口点，用于配置Hadoop作业。
  通过 Job 类，指定了Map和Reduce的类，以及输入和输出数据的格式。
  设置了输入文件路径和输出文件路径。
  最后，启动Hadoop作业，等待作业完成，然后退出。

  ![052eb730e38b723be586bf62847b938](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\052eb730e38b723be586bf62847b938.png)
  
  ![6b4c4f614c0f485fdf3b8bd6f1674c2](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\6b4c4f614c0f485fdf3b8bd6f1674c2.png)

---

## 1.3. 打包maven项目

```
1. 运行 Maven 编译和打包命令：
#在终端中，使用以下 Maven 命令来编译和打包你的项目：
mvn clean package
#这个命令会执行 clean 生命周期（清理旧构建），然后执行 package 生命周期，将项目打包为 JAR 文件。构建好的 JAR 文件将位于项目目录的 target 子目录下。
2. 查找 JAR 文件：
#构建成功后，你可以在项目目录的 target 文件夹中找到生成的 JAR 文件，通常以项目名称和版本号命名，如 my-java-project-1.0-SNAPSHOT.jar。
```

---

## 1.4. 在hadoop中运行程序
1. 准备伪分布式hadoop
```
start-all.sh
```
2. 将输入文件上传到Hadoop中并准备input及output文件夹
```
hdfs dfs -rm -r /user/cmh/input
hdfs dfs -mkdir /user/cmh/input
hdfs dfs -rm -r /user/cmh/output
#删除旧的input及output文件夹并创建新的input文件夹
hdfs dfs -put inputfile-path input
#input-path 是输入数据的路径
```
3. 使用 hadoop jar 命令来运行 JAR 文件
```
hadoop jar your-project.jar main-class input-path output-path
#your-project.jar 是你上传到 Hadoop 的 JAR 文件
#main-class 是包含 main 方法的 Java 类的全名，用于启动你的作业
#input-path 是输入数据的 HDFS 路径
#output-path 是输出结果的 HDFS 路径
```

## 1.5. 运行结果
```
hdfs dfs -ls /user/cmh/output
#查看是否运行成功（也可通过8088端口查看Hadoop ResourceManager web界面）
hdfs dfs -get output output
cat output/*
#检查输出：将分布式文件系统中的输出文件拷贝至本地文件系统并检查
```
![fc97f88be12158fcbf64ec0f725edf9](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\fc97f88be12158fcbf64ec0f725edf9-1698156577895-6.png)

![78facd88071b9b08984cfe37885785b](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\78facd88071b9b08984cfe37885785b.png)

输出文件（只展示了前37行）
![6c89a045495c4f732dc8ce3bbcc25b3](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\6c89a045495c4f732dc8ce3bbcc25b3.png)

---

## 1.6. 附：由于在本作业中涉及到了虚拟机中github的使用，因此将在虚拟机中科学上网的方法记录如下

1. 打开clash中的Allow LAN按钮，点击三角符号，记录WLAN的IP地址及端口号

   ![90ff1ab107f3548871fb3ed15c1510c](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\90ff1ab107f3548871fb3ed15c1510c.png)

   ![66c9ed3ba8c6f546f30702cd980b5d7](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\66c9ed3ba8c6f546f30702cd980b5d7.png)

   

2. 打开虚拟机右上角的settings-Network，将Network Proxy修改为manual，并在四行中填入之前记录的IP及端口号

   ![bb8885ad18779728138c4d3a09d771a](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\bb8885ad18779728138c4d3a09d771a.png)

   ![0cfaa5f7289e0bbd2caabc224226bfb](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\0cfaa5f7289e0bbd2caabc224226bfb.png)

   ![1955251b811fd0d47209d4449ab82da](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\homework5\1955251b811fd0d47209d4449ab82da.png)

3. 设置完成:happy: