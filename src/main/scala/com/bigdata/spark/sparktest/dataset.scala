package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object dataset {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataset").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val lines=ssc.socketTextStream("ec2-54-242-154-134.compute-1.amazonaws.com",port=1234)
    lines.print()

    import spark.implicits._
    import spark.sql

    ssc.start()
    ssc.awaitTermination()
   //spark.stop()
  }
}