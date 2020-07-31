package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkcsvdata {
  //case class Uscc(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:Int,phone1:String,phone2:String,email:String,web:String)
  case class Uscc1(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:Int,phone1:Int,phone2:Int,email:String,web:String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkcsvdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="C:\\work\\datasets\\us-500.csv"
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
   // val ds=df.as[Uscc]
    //ds.createOrReplaceTempView("tab")
    //val res=spark.sql("select * from tab where email like '%gmail%'")
    val res1=df.withColumn("phone1",regexp_replace($"phone1","_","")).withColumn("phone2",regexp_replace($"phone2","_",""))
      val ds1=res1.as[Uscc1]
     ds1.show()
     ds1.printSchema()
    spark.stop()
  }
}