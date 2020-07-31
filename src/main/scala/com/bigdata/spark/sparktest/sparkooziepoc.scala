package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkooziepoc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkooziepoc").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val url = "jdbc:oracle:thin:@//corona2020.cngsgxxckbvf.us-east-2.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df = spark.read.jdbc(url,"EMP",prop)
    //df.createOrReplaceTempView("tab")
    //val res=spark.sql("select * from tab")
    //res.show()
      //.where($"datecol">=current_date())
    //if(df.count()>0) {
      df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("EMP")
   //}
   // else "there is no records"
    spark.stop()
  }
}