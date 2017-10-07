package com.iii.sparksql101
import org.apache.spark.sql.SparkSession

object runCreateDfFromStructureFileExample extends App {
  
  val spark = SparkSession
    .builder()
    .appName("JSON and Parquet example")
    .getOrCreate()

  val df = spark.read.json("hdfs://localhost/user/cloudera/spark_sql_101/data/people.json")

  // DataFrames can be saved as Parquet files, maintaining the schema information
  df.write.parquet("hdfs://localhost/user/cloudera/spark_sql_101/data/people.parquet")

  // Read in the parquet file created above
  // Parquet files are self-describing so the schema is preserved
  // The result of loading a Parquet file is also a DataFrame
  val parquetFileDF = spark.read.parquet("hdfs://localhost/user/cloudera/spark_sql_101/data/people.parquet")

  // Parquet files can also be used to create a temporary view and then used in SQL statements
  parquetFileDF.createOrReplaceTempView("parquetFile")

  val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")

  namesDF.show()
}