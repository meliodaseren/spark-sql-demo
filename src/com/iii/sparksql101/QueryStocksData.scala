package com.iii.sparksql101

import org.apache.spark.sql.SparkSession

object QueryStocksData extends App {

  val spark = SparkSession
    .builder()
    .appName("Query Stocks Data")
    .getOrCreate()

  // Create a DataFrame from a JSON file
  val stocksDF = spark.read.json("hdfs://localhost/user/cloudera/spark_sql_101/data/stocks.json")

  // Displays the content of the DataFrame to stdout
  stocksDF.show()

  // Print the schema in a tree format
  stocksDF.printSchema()

  // Register the DataFrame as a SQL temporary view
  stocksDF.createOrReplaceTempView("stocks")

  val resultDF = spark.sql("SELECT symbol, avg(open) FROM stocks GROUP BY symbol")

  resultDF.show()

  // Write the resultDF to HDFS
  resultDF.write.json("hdfs://localhost/user/cloudera/spark_sql_101/result/")
}



