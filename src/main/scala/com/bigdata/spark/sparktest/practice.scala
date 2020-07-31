package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row._


object practice {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("practice").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="C:\\work\\datasets\\bank-full.csv"
    val brrd=sc.textFile(data)
    val fir=brrd.first()
    val res=brrd.filter(x=>x!=fir).map(x=>x.replaceAll("\"","")).map(x=>x.split(";")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    res.take(10).foreach(println)

    spark.stop()
  }
}