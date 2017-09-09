package com.iii.sparksql101
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object runBasicDataFrameExample extends App {
  
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  // $example on:create_df$
  val df = spark.read.json("hdfs://localhost/user/cloudera/spark_sql_101/data/people.json")

  // Displays the content of the DataFrame to stdout
  df.show()

  import spark.implicits._

  // Print the schema in a tree format
  df.printSchema()

  // Select only the "name" column
  df.select("name").show()

  // Select everybody, but increment the age by 1
  df.select($"name", $"age" + 1).show()

  // Select people older than 21
  df.filter($"age" > 21).show()

  // Count people by age
  df.groupBy("age").count().show()

  // Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")

  val sqlDF = spark.sql("SELECT * FROM people")
  sqlDF.show()

  // Register the DataFrame as a global temporary view
  df.createGlobalTempView("people")

  // Global temporary view is tied to a system preserved database `global_temp`
  spark.sql("SELECT * FROM global_temp.people").show()

  // Global temporary view is cross-session
  spark.newSession().sql("SELECT * FROM global_temp.people").show()
}