package com.iii.sparksql101.ec

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar

object Config {
  val records = 1000000
  val products = 200
  val referers = 20
  val pages = 2000
  val visitors = 50000
  val outputPath = "/home/cloudera/Desktop/spark_sql_101/ec/data/access.log"
  val dateFormat = "yyyy-MM-dd hh:mm:ss.S";
}

object LogGenerator extends App {

  val products = (0 to Config.products).map("product-" + _)
  val referers = (0 to Config.referers).map("referer-" + _)
  val pages = (0 to Config.pages).map("page-" + _)
  val visitors = (0 to Config.visitors).map("visitor-" + _)

  val rand = new scala.util.Random()
  val writer = new FileWriter(Config.outputPath, true)

  var timeStartFrom = System.currentTimeMillis()
  var calendar = Calendar.getInstance()
  val simpleDateFormat = new SimpleDateFormat(Config.dateFormat)
  
  for (round <- 1 to Config.records) {
    
    val action = rand.nextInt(1000) % 3 match {
      case 0 => "page_view"
      case 1 => "add_to_cart"
      case 2 => "sale"
    }
    
    val product = products(rand.nextInt(products.length - 1))
    val referrer = referers(rand.nextInt(referers.length - 1))
    val page = pages(rand.nextInt(pages.length - 1))
    val visitor = visitors(rand.nextInt(visitors.length - 1))

    calendar.setTimeInMillis(timeStartFrom)
    val timestamp = simpleDateFormat.format(calendar.getTime())
    timeStartFrom += rand.nextInt(6000)
    
    val line = s"$timestamp,$referrer,$action,$visitor,$page,$product\n"
    writer.write(line)
  }
  writer.close()
}
