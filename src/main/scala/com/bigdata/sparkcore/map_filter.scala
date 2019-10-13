package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession

object map_filter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val names= Array("venu","anand","all others")

val RDD = sc.parallelize(names)
    val process = RDD.map(x=>x.toUpperCase)
    process.collect.foreach(println)


    //val data = "file:///C:\\work\\datasets\\asldata.txt"
    val data=args(0)
    val aslrdd = sc.textFile(data)
    val first=aslrdd.first() // take only first line
    //val res = aslrdd.filter(x=>x!=first).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._2>50)
    // select * from asl where city="mas"
    val res = aslrdd.filter(x=>x!=first).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._3=="mas")
        //.............filter first line.split create array format .. convert to tuple.............. filger only mas info
    //Array("venu",40,mas)
    res.collect.foreach(println)




    spark.stop()
  }
}

