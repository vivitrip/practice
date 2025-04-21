package com.xw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MySQLToMaxCompute {

  public static void main(String[] args) {
    // MaxCompute 连接配置
    String odpsUrl = "http://service.cn.maxcompute.aliyun.com/api";
    String odpsProject = "spark_demo";
    String odpsTable = "test_user";
    String odpsAccessId = "*";
    String odpsAccessKey = "*";

    SparkSession spark = SparkSession.builder().appName("MySQLToMaxCompute")
        .master("local[*]")  // 生产环境替换为 "yarn" 或 "spark://<master-url>"
        .config("spark.sql.broadcastTimeout", "1200").config("spark.sql.crossJoin.enabled", "true")
        .config("odps.exec.dynamic.partition.mode", "nonstrict")
        // MaxCompute 连接信息
        .config("spark.odps.project.name", odpsProject).config("spark.odps.access.id", odpsAccessId)
        .config("spark.odps.access.key", odpsAccessKey).config("spark.odps.end.point", odpsUrl)

        .enableHiveSupport()
        .getOrCreate();

    Dataset<Row> mysqlDF = spark.read().format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/spark_demo")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "test_user")
        .option("user", "xw").option("password", "xwpwd").load();
    mysqlDF.show();

    spark.sql("SELECT current_database()").show();
    spark.sql("CREATE TABLE if not exists test_user (id BIGINT,name STRING,  age BIGINT)");
    mysqlDF.write().mode(SaveMode.Append)
        .insertInto(odpsTable);  // 注意：不需要加 project 前缀（即不要写 spark_demo.test_user）
    spark.sql("SELECT * FROM test_user").show();


    spark.stop();
  }
}