package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object inpatienttask {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("inpatienttask").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="E:\\Bigdata\\venutaskdatasets\\inpatientCharges.csv"
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",",").load(data)
   val reg="[^a-zA-Z]"
    val cols=df.columns.map(x=>x.replaceAll(reg,"")).map(x=>x.toLowerCase())
    val ndf=df.toDF(cols:_*)

    //val res=ndf.groupBy("ProviderState")
    ndf.show()

    spark.stop()
  }
}