package com.xw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MySQLToMaxCompute {

  public static void main(String[] args) {
    // 加载配置文件
    Properties properties = new Properties();
    try (FileInputStream fis = new FileInputStream("config.properties")) {
      properties.load(fis);
    } catch (IOException e) {
      System.err.println("Failed to load configuration file: " + e.getMessage());
      return;
    }

    // 从配置文件中读取配置
    String odpsUrl = properties.getProperty("odps.url");
    String odpsProject = properties.getProperty("odps.project");
    String odpsTable = properties.getProperty("odps.table");
    String odpsAccessId = properties.getProperty("odps.accessId");
    String odpsAccessKey = properties.getProperty("odps.accessKey");

    String mysqlUrl = properties.getProperty("mysql.url");
    String mysqlUser = properties.getProperty("mysql.user");
    String mysqlPassword = properties.getProperty("mysql.password");
    String mysqlTable = properties.getProperty("mysql.table");
    String partitionColumn = properties.getProperty("mysql.partitionColumn");
    int lowerBound = Integer.parseInt(properties.getProperty("mysql.lowerBound"));
    int upperBound = Integer.parseInt(properties.getProperty("mysql.upperBound"));
    int numPartitions = Integer.parseInt(properties.getProperty("mysql.numPartitions"));

    // 初始化 SparkSession
    SparkSession spark = SparkSession.builder().appName("MySQLToMaxCompute")
        .master("local[*]")  // 生产环境替换为 "yarn" 或 "spark://<master-url>"
        .config("spark.sql.broadcastTimeout", "1200")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("odps.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        // MaxCompute 连接信息
        .config("spark.odps.project.name", odpsProject)
        .config("spark.odps.access.id", odpsAccessId)
        .config("spark.odps.access.key", odpsAccessKey)
        .config("spark.odps.end.point", odpsUrl)
        .enableHiveSupport()
        .getOrCreate();

    // MySQL 数据读取配置，启用分区化读取
    Dataset<Row> mysqlDF = spark.read().format("jdbc")
        .option("url", mysqlUrl)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", mysqlTable)
        .option("user", mysqlUser)
        .option("password", mysqlPassword)
        .option("partitionColumn", partitionColumn)
        .option("lowerBound", lowerBound)
        .option("upperBound", upperBound)
        .option("numPartitions", numPartitions)
        .load();

    mysqlDF.show();

    // MaxCompute 数据写入，确保分区化写入
    spark.sql("CREATE TABLE if not exists test_user (id BIGINT, name STRING, age BIGINT) PARTITIONED BY (region STRING)");
    mysqlDF.write()
        .mode(SaveMode.Append)
        .insertInto(odpsTable);

    spark.sql("SELECT * FROM test_user").show();
    spark.sql("SELECT count(*) FROM test_user").show();

    spark.stop();
  }
}
