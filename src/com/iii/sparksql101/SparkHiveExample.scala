package com.iii.sparksql101

import org.apache.spark.sql.SparkSession

object SparkHiveExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Query Hive tables example")
    .enableHiveSupport()
    .getOrCreate()

  spark.sql("show tables").show()

  // Queries are expressed in HiveQL
  spark.sql("SELECT * FROM stocks").show()

  // Aggregation queries are also supported.
  spark.sql("SELECT symbol, count(*) FROM stocks GROUP BY symbol").show()
}



