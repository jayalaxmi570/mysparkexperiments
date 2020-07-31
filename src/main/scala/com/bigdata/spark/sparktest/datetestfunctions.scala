package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object datetestfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datetestfunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("dateFormat","MM/dd/yyyy").load(data)
    val reg="[^a-zA-Z0-9]"
    val cols=df.columns.map(x=>x.replaceAll(reg,"")).map(x=>x.toLowerCase())
    val ndf=df.toDF(cols:_*)
    val res = ndf.withColumn("dateofbirth", to_date(col("dateofbirth"),"MM/dd/yyyy")).withColumn("dateofjoining", to_date(col("dateofjoining"),"MM/dd/yyyy")).withColumn("yearsagediff",year(col("dateofjoining"))-year(col("dateofbirth"))).withColumn("ageindiff",round(datediff(col("dateofjoining"),col("dateofbirth"))/365))
    res.show()
   // val res=ndf.select("dateofbirth","dateofjoining")
    //val res2=res.select(res.columns.map(x=>to_date(col(x),"MM/dd/yyyy").alias(x)):_*)

    //val res3=res2.withColumn("today",current_date()).withColumn("currtime",current_timestamp()).withColumn("agediff",datediff(col("dateofjoining"),col("dateofbirth"))/365)
    //val res4=res2.withColumn("yearsagediff",year(col("dateofjoining"))-year(col("dateofbirth")))
        //val res=ndf.withColumn("dateofbirth",to_date(col("dateofbirth"),"MM/dd/yyyy")).withColumn("dateofjoining",to_date(col("dateofjoining"),"MM/dd/yyyy"))res4.show()
    //    res2.printSchema()
    //val reg = "[^a-zA-Z0-9]"
    //val cols = df.columns.map(x=>x.replaceAll(reg,"")).map(x=> x.toLowerCase()) // Array(" EMP id, ...")
    //val ndf =df.toDF(cols:_*)
   // val res = ndf.withColumn("dateofbirth", to_date(col("dateofbirth"),"MM/dd/yyyy")).withColumn("dateofjoining", to_date(col("dateofjoining"),"MM/dd/yyyy")).withColumn("yearsagediff",year(col("dateofjoining"))-year(col("dateofbirth")))
   //res.show()
    //res1.printSchema()

    spark.stop()
  }
}