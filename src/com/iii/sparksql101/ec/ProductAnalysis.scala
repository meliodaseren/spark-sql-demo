package com.iii.sparksql101.ec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

case class LogEntry(timestamp: String,
                    referrer: String,
                    action: String,
                    visitor: String,
                    page: String,
                    product: String)

object ProductAnalysis extends App {

  val spark = SparkSession
    .builder()
    .appName("ec")
    .getOrCreate()
  val sc = spark.sparkContext
  val sourceFile = "hdfs://localhost/user/cloudera/spark_sql_101/ec/data"
  val input = sc.textFile(sourceFile)

  import spark.implicits._
  
  val logsDF = input.flatMap { line =>
    val record = line.split(",")
    if (record.length == 6)
      Some(LogEntry(record(0), record(1), record(2), record(3), record(4), record(5)))
    else
      None
  }.toDF().cache()

  logsDF.createOrReplaceTempView("logs")

  val visitorsByProduct = spark.sql(
    """SELECT product, substring(timestamp, 0, 13) as date_hour, COUNT(DISTINCT visitor) as uu
       FROM logs 
       GROUP BY product, substring(timestamp, 0, 13)
      """).cache()

  val activityByProduct = spark.sql("""SELECT
                                    product,
                                    substring(timestamp, 0, 13) as date_hour,
                                    sum(case when action = 'sale' then 1 else 0 end) as number_of_sales,
                                    sum(case when action = 'add_to_cart' then 1 else 0 end) as number_of_add_to_cart,
                                    sum(case when action = 'page_view' then 1 else 0 end) as number_of_page_views
                                    from logs
                                    group by product, substring(timestamp, 0, 13) """).cache()

  visitorsByProduct.show(100)

  visitorsByProduct.write.mode(SaveMode.Overwrite)
                         .csv("hdfs://localhost/user/cloudera/spark_sql_101/ec/ouptut/visitors_by_product")

  activityByProduct.show(100)
  activityByProduct.write.mode(SaveMode.Overwrite)
                         .csv("hdfs://localhost:/user/cloudera/spark_sql_101/ec/ouptut/activity_by_product")
}
