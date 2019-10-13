package com.bigdata.sparkcore

import org.apache.spark.sql.SparkSession

object groupBy {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("groupBy").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
// select * from tab wherece city='mas"
    // select count(*) , city from tab group by city
    //val data ="file:///C:\\work\\datasets\\ca-500.csv"
    //val nRDD = sc.textFile(data)
    //val head = nRDD.first()
    //val regex= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    //val res = nRDD.filter(x=>x!=head).map(x=>x.split(regex))
      //.map(x=>(x(5).replaceAll("\"",""),1))
      //.reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)

    //res.take(10).foreach(println)
//val data = "file://C:\\work\dataset\ca-500.csv"
    //val nrdd = sc.textFile(data)
    //val head = nrdd.first()

    //val data = "C:\\work\\datasets\\us-500.csv"
    //val srdd = sc.textFile(data)
    //val head = srdd.first()
    //val ss = srdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(4).replaceAll("\"",""),"")).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    //srdd.map((s) => notSerializable.doSomething(s)).collect
    //ss.take(100).foreach(println)

    spark.stop()
  }
}

