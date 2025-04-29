package com.xw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MySQLToMaxCompute {

  public static Properties loadProperties(String filePath) throws IOException {
    Properties properties = new Properties();
    try (FileInputStream fis = new FileInputStream(filePath)) {
      properties.load(fis);
    }
    return properties;
  }

  public static SparkSession initializeSparkSession(Properties properties) {
    return SparkSession.builder().appName("MySQLToMaxCompute")
        .master("local[*]")
        .config("spark.sql.broadcastTimeout", "1200")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("odps.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.odps.project.name", properties.getProperty("odps.project"))
        .config("spark.odps.access.id", properties.getProperty("odps.accessId"))
        .config("spark.odps.access.key", properties.getProperty("odps.accessKey"))
        .config("spark.odps.end.point", properties.getProperty("odps.url"))
        .enableHiveSupport()
        .getOrCreate();
  }

  public static Dataset<Row> readMySQLData(SparkSession spark, Properties properties) {
    return spark.read().format("jdbc")
        .option("url", properties.getProperty("mysql.url"))
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", properties.getProperty("mysql.table"))
        .option("user", properties.getProperty("mysql.user"))
        .option("password", properties.getProperty("mysql.password"))
        .option("partitionColumn", properties.getProperty("mysql.partitionColumn"))
        .option("lowerBound", Integer.parseInt(properties.getProperty("mysql.lowerBound")))
        .option("upperBound", Integer.parseInt(properties.getProperty("mysql.upperBound")))
        .option("numPartitions", Integer.parseInt(properties.getProperty("mysql.numPartitions")))
        .load();
  }

  public static void writeToMaxCompute(Dataset<Row> dataset, String odpsTable) {
    dataset.write()
        .mode(SaveMode.Append)
        .insertInto(odpsTable);
  }

  public static void main(String[] args) {
    try {
      Properties properties = loadProperties("config.properties");
      SparkSession spark = initializeSparkSession(properties);
      Dataset<Row> mysqlDF = readMySQLData(spark, properties);
      mysqlDF.show();
      writeToMaxCompute(mysqlDF, properties.getProperty("odps.table"));
      spark.stop();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
