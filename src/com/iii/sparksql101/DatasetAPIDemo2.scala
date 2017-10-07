package com.iii.sparksql101

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import scala.util.Try

case class Account(number: String, firstName: String, lastName: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double,
                       description: String)
case class TransactionForAverage(accountNumber: String, amount: Double, description: String,
                                 date: java.sql.Date)

object DatasetAPIDemo2 {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Dataset API Demo")
      .enableHiveSupport
      .getOrCreate()

    import spark.implicits._

    val financesDS = spark.read.json("hdfs://localhost/user/cloudera/spark_sql_101/data/finances.json")
                          .withColumn("date", to_date(unix_timestamp($"Date","MM/dd/yyyy").cast("timestamp")))
                          .as[Transaction]

    val etlDS = financesDS
      .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
      .na.fill("Unknown", Seq("Description")).as[Transaction]
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .select($"Account.Number".as("AccountNumber").as[String], $"Amount".as[Double],
        $"Date".as[java.sql.Date](Encoders.DATE), $"Description".as[String])

    etlDS.show(100, false)

    if (Try(financesDS("_corrupt_record")).isSuccess) {
      val corruptDS = financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
      corruptDS.show(100, false)
    }

    val accountDF = financesDS
      .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date")).as[Transaction]
      .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}", tx.account.number))
      .distinct
      .toDF("FullName", "AccountNumber")
      .coalesce(2)

    accountDF.show(100, false)

    val accountDetailsDF = financesDS
      .select($"account.number".as("accountNumber").as[String], $"amount".as[Double],
        $"description".as[String],
        $"date".as[java.sql.Date](Encoders.DATE)).as[TransactionForAverage]
      .groupBy($"AccountNumber")
      .agg(avg($"Amount").as("average_transaction"), sum($"Amount").as("total_transactions"),
        count($"Amount").as("number_of_transactions"), max($"Amount").as("max_transaction"),
        min($"Amount").as("min_transaction"), stddev($"Amount").as("standard_deviation_amount"),
        collect_set($"Description").as("unique_transaction_descriptions"))
      .coalesce(2)

    accountDetailsDF.show(100, false)
  }
}