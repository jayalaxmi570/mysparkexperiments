package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object kafkaconsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("KafkaConsumer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val output="C:\\work\\kafkaop"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("indpak")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record =>  record.value)
    //lines.print()
    lines.saveAsTextFiles(output)
    lines.foreachRDD{ x=>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df= x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()
      df.createOrReplaceTempView("tab")
      val mas = spark.sql("select * from tab where city='mas'")
      val del = spark.sql("select * from tab where city='del'")
      val url = "jdbc:oracle:thin:@//corona2020.cngsgxxckbvf.us-east-2.rds.amazonaws.com:1521/ORCL"
      val prop = new java.util.Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.OracleDriver")
      mas.write.mode(SaveMode.Append).jdbc(url,"masinfo",prop)
      del.write.mode(SaveMode.Append).jdbc(url,"delhiinfo",prop)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}