package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession

object regex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("regex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    spark.stop()
  }
}

