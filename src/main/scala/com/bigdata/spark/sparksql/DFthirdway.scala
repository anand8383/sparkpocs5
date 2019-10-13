package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object DFthirdway {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DFthirdway").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val rdd =sc.textFile(data)

    val col = "first_name,last_name,company_name,address,city,county,state,zip,phone1,phone2,email,web"

    // Generate the schema based on the string of schema
    val fields = col.split(",").map(x => StructField(x, StringType, nullable = true))
    val schema = StructType(fields)
    val reg_exp= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val head = rdd.first()
    val raw = rdd.filter(x=>x!=head).map(x=>x.split(reg_exp)).map(x=>Row(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"","")))

    val df = spark.createDataFrame(raw,schema)
    df.show()


    spark.stop()
  }
}

