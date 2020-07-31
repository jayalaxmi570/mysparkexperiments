package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object delayflighttask {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("delayflighttask").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data="E:\\Bigdata\\venutaskdatasets\\DelayedFlights.csv"
    val rdd=sc.textFile(data)
    val fir=rdd.first()
    val res=rdd.filter(x=>x!=fir).map(x=>x.split(",")).filter(x=>x!=null).map(x=>(x(17),x(18),x(24)))
    val df=res.toDF("orign","dest","diversion")
    val res1=df. select("*").where($"diversion"==="1")
    val res2=res1.groupBy(col("orign"),col("dest")).count().orderBy($"count".desc)
    res2.show()

      //assgn3
     // .map(x=>x(18))
   // val df=res.toDF("destination")
   // val res1=df.groupBy($"destination").count().orderBy($"count".desc)
    //assgn 1
      //.map(x=>(x(2),x(22),x(23)))
    // val df=res.toDF("month","cancelled","cancellation")
    //val res1=df.select("*").where($"cancelled" ==="1" && $"cancellation"==="B")
   //val res2=res1.groupBy($"month").count().orderBy($"count".desc)
     //res.take(10).foreach(println)
    //res1.show()


   //res2.show()



    spark.stop()
  }
}
