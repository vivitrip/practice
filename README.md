# 练习项目：使用Spark完成从mysql读取数据并写入MaxCompute的过程
## 当前进度
- [x] 主流程：从mysql读取数据并写入MaxCompute
- [x] 配置独立：每次执行任务时候从配置读取Spark、Mysql、MaxCompute的配置信息
- [ ] 增加单元测试

## 思路拆分和工程代码说明

### 思路拆分
1. 首先需要完成流程环境准备：
   1. mysql：基于docker的本地服务
   2. spark：基于docker的本地服务
   3. maxCompute：阿里云申请的免费试用
2. 完成spark从mysql读取数据的过程
3. 完成spark向maxCompute写入数据的过程
4. 工程完善：
   1. 配置和代码独立
   2. 增加单元测试

### 操作步骤
#### 基于docker安装本地mysql
使用docker desktop在macbook（m2芯片）平台安装。

编写docker-compose文件和数据初始化脚本
```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: mysql8-spark
    restart: always
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: spark_demo
      MYSQL_USER: xw
      MYSQL_PASSWORD: xwpwd
    volumes:
      - ./mysql/data:/var/lib/mysql
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
```
```sql
CREATE DATABASE spark_demo;
USE spark_demo;
CREATE TABLE test_user (
                           id INT PRIMARY KEY,
                           name VARCHAR(100),
                           age INT
);

INSERT INTO test_user (id, name, age) VALUES
                                          (1, 'Alice', 25),
                                          (2, 'Bob', 30),
                                          (3, 'Charlie', 22),
                                          (4, 'Daniel', 32),
                                          (5, 'Edison', 18)
;
```
验证安装完成
```shell
mysql -h 127.0.0.1 -P 3306 -u xw -pxwpwd spark_demo
```
```sql
select * from spark_demo.test_user;
```
可以看到数据输出。

#### 基于docker完成spark安装
编写docker-compose文件。

```yaml
version: '3'
services:
  spark-master:
    image: bitnami/spark:2.4.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no

    hostname: spark-master

    ports:
      - "8080:8080"   # Spark Master UI
      - "7077:7077"   # Spark Master RPC
    networks:
      - spark-net

  spark-worker:
    image: bitnami/spark:2.4.5
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=1G
    depends_on:
      - spark-master
    ports:
      - "8081:8081"   # Spark Worker UI
    networks:
      - spark-net

networks:
  spark-net:
    driver: bridge
```
验证服务启动正常，访问http://127.0.0.1:8080/和http://127.0.0.1:8081/，验证master和worker都启动正常。

在本地安装spark cli用于任务提交。
```shell
brew install apache-spark
spark-submit --version
```

执行example任务：
```shell
spark-submit --class org.apache.spark.examples.SparkPi \
  --master "local[*]" \
  /opt/homebrew/opt/apache-spark/libexec/examples/jars/spark-examples_2.12-3.5.5.jar 10
```
可以看到输出Pi值
```
Pi is roughly 3.1424591424591424
```

### 编写从mysql读取数据的代码

参看 App.java。

打包后可用以下命令测试：
```shell
spark-submit --class com.xw.App \
  --master "local[*]" \
 target/practice-1.0-SNAPSHOT-jar-with-dependencies.jar
```


### 编写从mysql读取数据并写入maxCompute的代码

参看 MySQLToMaxCompute.java,代码中隐藏了access key的信息。

打包后可用以下命令测试

```shell
spark-submit --class com.xw.MySQLToMaxCompute \
  --master "local[*]" \
 target/practice-1.0-SNAPSHOT-jar-with-dependencies.jar
```