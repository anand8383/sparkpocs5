package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparksql.sparkallfunctions._
object getmysqloracledata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getmysqloracledata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
 val myurl = "jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop =new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(myurl,"emp",oprop)
    df.show()

    val mysql = "jdbc:mysql://mysqldb.clbm1tfz6ih0.us-east-2.rds.amazonaws.com:3306/mysqldb"
    val oop = new java.util.Properties()
    oop.setProperty("user","musername")
    oop.setProperty("password","mpassword")
    oop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val dff = spark.read.jdbc(mysql,"dept ",oop)
    dff.show()
    def empdf = df
     df.createOrReplaceTempView("emp")
    dff.createOrReplaceTempView("dept")
    //val res = spark.sql (       "select e.ename, e.empno, e sal, d.loc from emp e join dept d on d.deptno=e.deptno")
    val res = spark.sql ("select e.ename, e.empno, e.sal, d.loc from emp e join dept d on d.deptno=e.deptno")
 res.show()
    val saludf = udf(salgrade _)
val sgres = df.withColumn("grade", saludf($"sal"))
    sgres.show()

 val mm = "jdbc:mysql://mysqldb.clbm1tfz6ih0.us-east-2.rds.amazonaws.com:3306/mysqldb"
    val mydriver = new java.util.Properties()
    mydriver.setProperty("user","musername")
    mydriver.setProperty("password","mpassword")
    mydriver.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val myres = spark.read.jdbc(mm,"emp",mydriver)
    val ss= spark.sql("select * from emp where empno like '7%'")
//ss.show()
    spark.stop()
  }
}

