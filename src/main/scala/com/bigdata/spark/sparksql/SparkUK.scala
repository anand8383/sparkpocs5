package com.bigdata.spark.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkUK {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkUK").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\uk-500.csv"
    val rr = sc.textFile(data)
    val coll = "first_name,last_name,company_name,address,city,county,postal,phone1,phone2,email,web"
    val field1 =coll.split(",").map(s=>StructField(s,StringType,nullable = true))
    val sch = StructType(field1)
    val reg_exp= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val head = rr.first()
    val raww = rr.filter(x=>x!=head).map(x=>x.split(reg_exp)).map(x=>Row(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"","")))

    //val res = rr.filter(x=>x!=head).map(x=>x.split(reg_exp)).map(x=>Row(x(0).replaceAll("\"","")))
    val dff = spark.createDataFrame(raww,sch)
    dff.show()


     // map(x=>Row(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"","")))


    spark.stop()
  }
}

