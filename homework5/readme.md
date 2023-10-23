# 作业5说明
**211275009 陈铭浩**

---

## maven创建项目后，更改pom.xml相关配置
```
#使用maven-archetype-quickstart(version=1.4)
1. 修改编译使用的jdk版本（本机为JDK20）
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>20</maven.compiler.source>
    <maven.compiler.target>20</maven.compiler.target>
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
    <dependency>
      <groupId>org.apache.poi</groupId>
      <artifactId>poi</artifactId>
      <version>5.2.4</version>
    </dependency>
  </dependencies>
```

## 打包maven项目
```
1. 运行 Maven 编译和打包命令：
#在终端中，使用以下 Maven 命令来编译和打包你的项目：
mvn clean package
#这个命令会执行 clean 生命周期（清理旧构建），然后执行 package 生命周期，将项目打包为 JAR 文件。构建好的 JAR 文件将位于项目目录的 target 子目录下。
2. 查找 JAR 文件：
#构建成功后，你可以在项目目录的 target 文件夹中找到生成的 JAR 文件，通常以项目名称和版本号命名，如 my-java-project-1.0-SNAPSHOT.jar。
```

