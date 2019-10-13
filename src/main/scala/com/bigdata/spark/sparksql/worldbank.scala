package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object worldbank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("worldbank").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
// except strings and integer will be removed.
    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\world.json"
    val df = spark.read.format("json").option("header",true).option("inferSchema",true).load(data)
    //df.createOrReplaceTempView(json)
   // df.show()
    df.filter($"_corrupt_record".isNotNull).show(false)
    df.printSchema()
    spark.stop()

  }
}

