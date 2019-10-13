package com.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object newwordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newwordcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val ss = sc.textFile("C:\\work\\datasets\\helloword.txt")
    //val xx = ss.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortBy(x=>x._2)
    //xx.toDebugString
    //xx.cache()
    //xx.collect.foreach(println)

    val ss = sc.textFile("c:\\work\\datasets\\chat1.rtf")
    val reg_exp= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
/*
    val vv = ss.map(x=>x.split(reg_exp)).replaceAll( "\"","").map(h=>(h,1)).reduceByKey(_+_).sortBy(x=>x._2)
    vv.toDebugString
    vv.cache()
    vv.collect.foreach(println)

*/


    spark.stop()
  }
}

