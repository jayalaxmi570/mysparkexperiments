package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jayatest1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jayatest1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="C:\\work\\datasets\\bank-full.csv"
    /*val brrd=sc.textFile(data)
    val fir=brrd.first()
   //val res=brrd.filter(x=>x!=fir).map(x=>x.split(";")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
   val reg="[^a-zA-Z]"
    val res=brrd.filter(x=>x!=fir).map(x=>x.split(",")).map(x=>(x(7).replaceAll(reg,""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2)
      //.map(x=>(x(0).toInt,x(1),x(2).replaceAll("\"",""))).filter(x=>x._1>30 && x._3=="divorced")
      //.filter(x=>x._1>30 && x._3=="divorced")

    res.take(100).foreach(println)*/
    val df=spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load(data)
    //val res=df.withColumn("state1",when($"state" ==="OH","Ohahio" ).otherwise("nill"))
    //val res=df.drop("state","city","zip")
    //val res1=df.filter(($"county" === "Orleans" && ))
   // val res=df.filter($"age" > 50 && $"marital"=== "single").withColumnRenamed("age","myage").withColumn("age1",lit(19))
    //val res1=df.where($"marital"=== "single" && $"age" > 55).withColumnRenamed("job","acupation").withColumn("age123",lit("yes")).withColumn("pdays",regexp_replace($"pdays","-1","nill"))
    val res1=df.withColumn("age1",when($"age" > 50,"senoir").otherwise("young"))
    res1.show()
    df.printSchema()


    spark.stop()
  }
}