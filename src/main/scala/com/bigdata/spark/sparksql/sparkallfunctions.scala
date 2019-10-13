package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkallfunctions {

  def salgrade(sal:Double)= sal match {
    case x if(x>1 && x<=1000)=> "1000 rupees off"
    case x if(x>1000 && x<3000)=>"10%off"
    case x if(x>=3000) => "20%off"
    case _ => "30% off"
  }

  def octoff (state:String) = state match {
    case "OH"=> "30% off"
    case "NJ"=> "20% off"
    case ("CA" | "TX") => "50% off"
    case _ => "5% off"
  }

}

