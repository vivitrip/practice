package com.xw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
        .appName("SparkMySQLExample")
        .getOrCreate();


    Dataset<Row> df = spark.read()
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/spark_demo")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "test_user") // 替换为你实际的表名
        .option("user", "xw")
        .option("password", "xwpwd")
        .load();

    df.show();
    spark.stop();
  }
}
