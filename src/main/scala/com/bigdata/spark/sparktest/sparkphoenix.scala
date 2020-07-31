package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkphoenix {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkphoenix").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   val tab=args(0)
    val df=spark.read.format("org.apache.phoenix.spark").option("table","tab").option("zkUrl","localhost:2181").load()
    df.show()

    spark.stop()
  }
}