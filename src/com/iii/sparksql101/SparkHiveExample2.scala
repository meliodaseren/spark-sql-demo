package com.iii.sparksql101

import org.apache.spark.sql.SparkSession

object SparkHiveExample2 extends App {

  val spark = SparkSession
    .builder()
    .appName("Query Hive tables example")
    .enableHiveSupport()
    .getOrCreate()

  spark.sql("CREATE TEMPORARY FUNCTION get_geo_locations AS 'com.iii.udf.GetGeoLocations'")

  // Queries are expressed in HiveQL
  spark.sql(
    """SELECT region,SUM(imp),COUNT(DISTINCT imp_imei),SUM(click),COUNT(DISTINCT click_imei)
      FROM
      (
        SELECT
        region,
        CASE WHEN log_type=1 THEN 1 ELSE 0 END AS imp,
        CASE WHEN log_type=1 THEN imei ELSE null END AS imp_imei,
        CASE WHEN log_type=2 THEN 1 ELSE 0 END AS click,
        CASE WHEN log_type=2 THEN imei ELSE null END AS click_imei
        FROM report_input
        LATERAL VIEW explode(split(get_geo_locations(ip_quadkey),',')) subview AS region
      ) src
      GROUP BY region
    """).show()
}
