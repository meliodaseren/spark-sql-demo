package com.iii.sparksql101

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Try

object DatasetAPIDemo1 {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Dataset API Demo")
      .enableHiveSupport
      .getOrCreate()

    import spark.implicits._
    val financesDF = spark.read.json("hdfs://localhost/user/cloudera/spark_sql_101/data/finances.json")

    val etlDF = financesDF
      .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
      .na.fill("Unknown", Seq("Description"))
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .selectExpr("Account.Number as AccountNumber", "Amount", "Date", "Description")

    etlDF.show(100, false)

    if (Try(financesDF("_corrupt_record")).isSuccess) {
      val corruptDF = financesDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
      corruptDF.show(100, false)
    }

    val accountDF = financesDF
      .select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"),
        $"Account.Number".as("AccountNumber"))
      .na.drop("all")
      .distinct
      .coalesce(2)

    accountDF.show(100, false)

    val accountDetailsDF = financesDF
      .select($"Account.Number".as("AccountNumber"), $"Amount", $"Description", to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp")).as("Date"))
      .na.drop("all", Seq("AccountNumber", "Amount", "Description", "Date"))
      .groupBy($"AccountNumber")
      .agg(avg($"Amount").as("AverageTransaction"), sum($"Amount").as("TotalTransactions"),
        count($"Amount").as("NumberOfTransactions"), max($"Amount").as("MaxTransaction"),
        min($"Amount").as("MinTransaction"), stddev($"Amount").as("StandardDeviationAmount"),
        collect_set($"Description").as("UniqueTransactionDescriptions"))
      .coalesce(2)

    accountDetailsDF.show(100, false)
  }
}