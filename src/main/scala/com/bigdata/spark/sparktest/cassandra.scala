package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.bigdata.spark.sparktest.importAll._


object cassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("cassandra").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","emp").load()
    val mdf=spark.read.jdbc(myurl,"emp",myprop)
    val odf=spark.read.jdbc(ourl,"dept",oprop)
    mdf.createOrReplaceTempView("emp")
    odf.createOrReplaceTempView("dept")
    val res=spark.sql("select e.*,d.loc from emp e join dept d on e.deptno=d.deptno")
    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","ejoind").save()

      res.show()
    spark.stop()
  }
}