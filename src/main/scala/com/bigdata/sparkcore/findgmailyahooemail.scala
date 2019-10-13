package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object findgmailyahooemail {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("findgmailyahooemail").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ss = "file:///C:\\work\\datasets\\tenthousandrecord.csv"
    val RR = sc.textFile(ss)
    val myres = RR.flatMap(x=>x.split(" ")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))//.filter(_.contains("gmail") )
    myres.collect.foreach(println)

    // .flatMap(x=>x.contains("yahoo.com"))


    //myrdd.flatMap(x=>x.split(" "))
    //        .flatMap(x=>x.split("}")).filter(x=>x.contains(".com")).flatMap(x=>x.split("http"))
    //    .flatMap(x=>x.split("://"))
    //

    spark.stop()
  }
}

