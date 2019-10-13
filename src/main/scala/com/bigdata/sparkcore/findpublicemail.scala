package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object findpublicemail {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("findpublicemail").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data2 = "file///C:\\work\\datasets\\chat1.rtf"
    val myrdd = sc.textFile(data2)
    val op = "file:///c:\\work\\datasets\\findpublicmail"
    //val myhead = myrdd.first()
    val ghh = myrdd.flatMap(x=>x.split(" "))
        .flatMap(x=>x.split("}")).filter(x=>x.contains(".com")).flatMap(x=>x.split("http"))
    .flatMap(x=>x.split("://"))
      ghh.collect.foreach(println)
    //ghh.coalesce(1 ).saveAsTextFile(op)
    spark.stop()
  }
}

