package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jsondata {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data  = "C:\\work\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
    df.show()
    df.printSchema()
    spark.stop()
  }
}