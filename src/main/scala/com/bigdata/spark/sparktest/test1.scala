package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object test1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracleImportAllToMssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin:@//corona2020.cngsgxxckbvf.us-east-2.rds.amazonaws.com:1521/ORCL"
    // GET DATA FROM ONE TABLE
    //val odf = spark.read.format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.driver.OracleDriver")
    //    .option("dbtable","EMP").load()

    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")

    val msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    // GET DATA BASED ON QUERY USE THIS OPTION
    //val query = "(select * from EMP where sal>2000) abcd"
    //  val odf = spark.read.jdbc(ourl,query,oprop)
    // IMPORT multiple TABLES FROM ORACLE
    /* val tabs = Array("EMP","DEPT")
     //foreach ... apply a function on top of each and every element but not return anything .. its acts like a for loop
     tabs.foreach{x=>
       val df1 = spark.read.jdbc(ourl,s"$x",oprop)
       df1.show()
     }*/
    // import all tables
    val query = "(select TABLE_NAME from all_tables WHERE TABLESPACE_NAME='USERS') t"
    val tabs = spark.read.jdbc(ourl,query,oprop).rdd.map(x=>x(0)).collect.toList.filter(x=>x!="EMP")

    tabs.foreach{x=>
      val df1 = spark.read.jdbc(ourl,s"$x",oprop)
      df1.show()
      df1.write.jdbc(msurl,s"$x",msprop)
    }

    spark.stop()
  }
}