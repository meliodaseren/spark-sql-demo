package com.iii.sparksql101
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object runInferSchemaExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  // For implicit conversions from RDDs to DataFrames
  import spark.implicits._

  // Create an RDD of Person objects from a text file, convert it to a Dataframe
  val peopleDF = spark.sparkContext
    .textFile("hdfs://localhost/user/cloudera/spark_sql_101/data/people.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()

  // Register the DataFrame as a temporary view
  peopleDF.createOrReplaceTempView("people")

  // SQL statements can be run by using the sql methods provided by Spark
  val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

  // The columns of a row in the result can be accessed by field index
  teenagersDF.map(teenager => "Name: " + teenager(0)).show()

  // or by field name
  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
}