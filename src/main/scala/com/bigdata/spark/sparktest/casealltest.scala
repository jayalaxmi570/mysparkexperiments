package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparktest.caseall._

object casealltest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("casealltest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   val data="C:\\work\\datasets\\10000Records.csv"
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val reg="[^a-zA-Z0-9]"
    val cols=df.columns.map(x=>x.replaceAll(reg,"")).map(x=>x.toLowerCase())
    val ndf=df.toDF(cols:_*)
    //ndf.show()
    //ndf.printSchema()
    val ds=ndf.as[Caseall]
    ds.createOrReplaceTempView("tab")
    val res=spark.sql("select * from tab where ageinyrs>=25 ")
    res.show()
    res.printSchema()
    //ds.createOrRe
    spark.stop()
  }
}