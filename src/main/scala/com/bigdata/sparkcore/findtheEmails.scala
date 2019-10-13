package com.bigdata.sparkcore
//com.bigdata.sparkcore.findtheEmails
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object findtheEmails {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("findtheEmails").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
// anandrajaca@gmail.com confuse bro.. pls explain

    import spark.implicits._
    import spark.sql

    //val data = "file:///C:\\work\\datasets\\chat1.rtf"
    val data=args(0) // input or output elements submit dinamically
 val op = args(1)
    //val op =  "file:///C:\\work\\datasets\\mails"
    val brdd = sc.textFile(data)
//val
    val res = brdd.flatMap(x=>x.split(" ")).flatMap(x=>x.split("}")).filter(x=>x.contains(".com"))
      .filter(x=>(!x.contains("https")) && (!x.contains("http")))
    res.collect.foreach(println)
    res.coalesce(1).saveAsTextFile(op)
    spark.stop()
  }
}
// will input from users. if user require 2 or 3 parameter during the excusion
// OOZIE is nothing but Auto schedule  engine & tools: