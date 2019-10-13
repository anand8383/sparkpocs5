package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("test1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val names= Array("venu","anand","super")
    names.map(x=>x.toUpperCase)
    spark.stop()
  }
}

