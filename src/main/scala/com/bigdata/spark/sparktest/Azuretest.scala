package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Azuretest {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Azuretest").getOrCreate()
   val spark = SparkSession.builder.appName("Azuretest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val url="jdbc:mysql://mysql.ckvslohv4g42.us-east-1.rds.amazonaws.com:3306/mysqldb"
    val prop=new java.util.Properties()
    prop.setProperty("user","musername")
    prop.setProperty("password","mpassword")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    //val df=spark.read.jdbc(url,"emp",prop)
    val df=spark.read.jdbc(url,args(0),prop)
   df.write.format("csv").option("header","true").save(args(1))
    df.show()
    spark.stop()
  }
}