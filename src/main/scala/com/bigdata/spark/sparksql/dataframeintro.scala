package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//first 3 ways .. just convert rdd to dataframe  (todf, case class, progra sch schema)
object dataframeintro {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataframeintro").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\work\\datasets\\us-500.csv"
    val rdd = sc.textFile(data)
    val reg_exp= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val head = rdd.first()
    val raw = rdd.filter(x=>x!=head).map(x=>x.split(reg_exp)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""))).toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    //toDF its convert rdd to datarame, but rdd must be structured format
    raw.createOrReplaceTempView("newdataframe")
    val df = spark.sql("select state, count(*) cnt from newdataframe group by state order by cnt desc")
    df.show()
    spark.stop()
  }
}

