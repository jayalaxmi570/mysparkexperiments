package com.bigdata.spark.sparktest

import org.apache.kafka.clients.producer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object kafkaproducerdemoobj {
  def main(args: Array[String]) {
    val producerProps = new java.util.Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("client.id", "SampleProducer")
    producerProps.put("acks", "all")
    producerProps.put("retries", new Integer(1))
    producerProps.put("batch.size", new Integer(16384))
    producerProps.put("linger.ms", new Integer(1))
    producerProps.put("buffer.memory", new Integer(133554432))

    val producer = new KafkaProducer[String, String](producerProps)

    for (a <- 1 to 2000) {

      val record: ProducerRecord[String, String] = new ProducerRecord("test1", s"name$a, name$a@gmail.com,area$a")
      producer.send(record);
      println(record.value())
      Thread.sleep(2000)
      // kafka send  million msg per seconds so all messages 2000 send within seconds thats to slow down use Thread.sleep
      //if u use Thread.sleep(2000) every 2 seconds it generate code
    }
    producer.close()

  }
}