package com.iii.sparksql101

import org.apache.spark.sql.SparkSession

object SparkHiveExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sql

  // Queries are expressed in HiveQL
  sql("SELECT * FROM stocks").show()

  // Aggregation queries are also supported.
  sql("SELECT symbol, count(*) FROM stocks GROUP BY symbol").show()

  spark.stop()
}