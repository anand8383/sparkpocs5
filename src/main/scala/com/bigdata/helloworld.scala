package com.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object helloworld {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("helloworld").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\helloword.txt"
    val sample_rdd=sc.textFile(data)
    val pp = sample_rdd.flatMap(x=>x.split(" " )).map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2)
      //.sortBy(x=>x._2, true)
        //reducebykey used to group the values its like select count(*) , city from tab group by city order by count desc.... like that if you want group by...
    pp.collect.foreach(println)
    spark.stop()
  }
}
/*
map: apply a logic on top of each element. input and output elements must be same... Array("venu",32,hyd") .... output also 3 lements .80% use map
filter: based on true/false values apply a logic on top of each element. input output may not equals.
flat map: map+ flattern ... means apply map first next flatter the results. input and output elements not same

reducebykey: group the similar elements based on key. its like select count(*), city from tab group by city like this to grop value use it.



 */

