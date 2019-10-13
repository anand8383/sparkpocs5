package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.functions._

object dataframeapi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataframeapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\zip.json"
    val df = spark.read.format("json").option("header",true).option("inferSchema",true).load(data)
    //df.createOrReplaceTempView(json)
    df.show()
    df.createOrReplaceTempView("tab")
    //val result = spark.sql("select * from tab")
    val result1 =spark.sql("select city,count(*) cnt from tab group by city order by cnt desc")
    df.printSchema()
    spark.stop()
  }
}

