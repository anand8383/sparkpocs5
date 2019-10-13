package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object findusEmail {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("findusEmail").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val rdd = sc.textFile(data)
    //val resul = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).filter(x=>x.contains(".com"))
      //  .filter(x=>x.contains(".net")).filter(x=>x.contains(".org"))
      val reg_exp= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val rrs = rdd.map(x=>x.split(reg_exp)).map(x=>(x(10).replaceAll("\"",""))).filter(x=>x.contains(".com"))
      //.filter(x=>x.contains(".net")).filter(x=>x.contains(".org"))
    //|| x._1.contains(".org") || x._1.contains(".net")))

//val myp = rdd.map(x=>x.split("reg_exp")).map(x=>x(10)).filter(x=>x.contains(".com"))
  rrs.collect.foreach(println)
// map = for structure data
    // flatMap = for unstructure data.


      //= brdd.flatMap(x=>x.split(" ")).flatMap(x=>x.split("}")).filter(x=>x.contains(".com"))
      //.filter(x=>(!x.contains("https")) && (!x.contains("http")))
    spark.stop()
  }
}

