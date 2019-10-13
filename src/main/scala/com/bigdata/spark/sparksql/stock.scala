package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object stock {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("stock").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val myjson = "file:///c:\\work\\datasets\\stock.json"
    val stocks = spark.read.format("json").option("inferSchema","true").load(myjson)
    val res= stocks.createOrReplaceTempView("tab")
    val reg = "[^\\p{L}\\p{Nd}]+"
    val mm = stocks.columns.map(x=>x.replaceAll(reg,""))
   val df = stocks.toDF(mm:_*)
    df.show()
   df.printSchema()
    spark.stop()
  }
}

