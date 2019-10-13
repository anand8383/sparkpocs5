package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object fourthwayDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("fourthwayDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header", "true").load(data)
    df.show()

    spark.stop()
  }
}

