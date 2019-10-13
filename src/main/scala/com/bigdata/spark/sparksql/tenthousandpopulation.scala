package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object tenthousandpopulation {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("tenthousandpopulation").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val jj= "file:///C:\\work\\datasets\\tenthousandrecord.csv"
    val mm = spark.read.format("csv").option("header",true).option("inferschema",true).load(jj)
    mm.createOrReplaceTempView("mytable")
    val result3 = spark.sql ("select EMP Id, First Name, [E Mail] as email from mytable where email like '%gmail%' AND email like '%Yahoo%'")
    result3.show(5)
    spark.stop()
  }
}

