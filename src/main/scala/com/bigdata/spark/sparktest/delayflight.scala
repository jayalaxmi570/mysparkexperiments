package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object delayflight {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("delayflight").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    // load the dataset using the textFile method.
    val delayed_Flights_data_with_header = spark.sparkContext.textFile("E:\\Bigdata\\venutaskdatasets\\DelayedFlights.csv")

    // load the dataset using the textFile method.

    //creating a variable header, which holds the first line of the dataset, in our data set Sports_data.txt
    // the first line is a header line.
    val header = delayed_Flights_data_with_header.first()

    //filter the header line from the dataset using the filter RDD
    val delayed_Flights_data = delayed_Flights_data_with_header.filter(row => row != header)

    // filter the null records from delauyed flight data
    val filter_null_values = delayed_Flights_data.map(x => x.split(",")).filter(x => x!= null)

    // filter diversion column with value 1 or "yes" and map corresponding orign and dest column as key
    val a = filter_null_values.filter(x => x(24).equals("1")).map(x => ((x(17)+ ","+ x(18)),1))

    //count all the values and sort it in descending order
    val b = a.reduceByKey((s,t)=>s+t).map(x => (x._2,x._1)).sortByKey(false)

    // print the maximum diversion for top 5 routes
    val c = b.map(x => (x._2,x._1)).take(5).foreach(println)
       spark.stop()
  }
}