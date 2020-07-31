package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object devfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("devfunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets1\\10000Records.csv"
   val df= spark.read.format("csv").option("header","true").option("inferSchema","true").option("dateFormat","MM/dd/yyyy").load(data)
    val reg = "[^a-zA-Z0-9]"
    val cols = df.columns.map(x=>x.replaceAll(reg,"")).map(x=> x.toLowerCase()) // Array(" EMP id, ...")
    val ndf =df.toDF(cols:_*)
    val res = ndf.withColumn("dateofbirth", to_date(col("dateofbirth"),"MM/dd/yyyy")).withColumn("dateofjoining", to_date(col("dateofjoining"),"MM/dd/yyyy"))
    // apply a logic on top each and eery column
    val res1 = ndf.select("dateofbirth","dateofjoining")
    val res2 = res1.select(res1.columns.map(x=>to_date(col(x),"MM/dd/yyyy").alias(x)):_*)


      //.withColumn("today",current_date()).withColumn("currts", current_timestamp())
      //.withColumn("ageWhenJoining",datediff(col("dateofjoining"),col("dateofbirth"))/365)
      // .withColumn("ageinyears",year(col("dateofjoining"))-year(col("dateofbirth")))
      // .withColumn("monthsbet",months_between(col("dateofjoining"),col("dateofbirth")))
      //  .withColumn("after50days",date_add(current_date(),50))
      //  .withColumn("addsub", date_sub(current_date(),50))
      //  .withColumn("nextSunday",next_day(current_date(),"Sunday"))
      //  .withColumn("weekofyear",weekofyear(col("dateofjoining")))
      // .withColumn("datetruncate",to_date(date_trunc("month",current_date())))
      //.withColumn("lastday",last_day(col("dateofjoining")))
     /* val res3 = res2.withColumn("lastfriday" , date_sub(next_day(current_date(),"friday"),7))
      .withColumn("qtr",quarter(col("dateofjoining")))
      .withColumn("dayofweek",dayofweek(col("dateofjoining")))
      .withColumn("dayofmonth",dayofmonth(col("dateofjoining")))
      .withColumn("dayofyear",dayofyear(col("dateofjoining")))
      .withColumn("dayofweek",dayofweek(col("dateofjoining")))
      .withColumn("uxts",unix_timestamp(col("dateofjoining")))
      .withColumn("convutxtodate",to_date(from_unixtime(col("uxts"))))*/
val res3=res2.withColumn("after28days",date_add(current_date(),28))

    res3.show()
    res3.printSchema()
    spark.stop()
  }
}