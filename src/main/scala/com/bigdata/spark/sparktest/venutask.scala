package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object venutask {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("venutask").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val myurl = "jdbc:mysql://mysql.ckvslohv4g42.us-east-1.rds.amazonaws.com:3306/mysqldb"
    // GET DATA FROM ONE TABLE
    //val odf = spark.read.format("jdbc").option("url",ourl).option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.driver.OracleDriver")
    //    .option("dbtable","EMP").load()



    val msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb"
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
    val myurl = "jdbc:mysql://mysql.ckvslohv4g42.us-east-1.rds.amazonaws.com:3306/mysqldb"
    val myprop = new java.util.Properties()
    myprop.setProperty("user","musername")
    myprop.setProperty("password","mpassword")
    myprop.setProperty("driver","com.mysql.cj.jdbc.Driver")

    val msquery = "(SELECT name FROM SYSOBJECTS WHERE   xtype = 'U') t"
    val tabs = spark.read.jdbc(msurl,msquery,msprop).rdd.map(x=>x(0)).collect.toList.filter(x=>x!="EMP")

    tabs.foreach{x=>
      val df1 = spark.read.jdbc(msurl,s"$x",msprop)
      df1.show()
      df1.write.mode(SaveMode.Overwrite)jdbc(myurl,s"$x"+"jaya",myprop)
    }

    spark.stop()
  }
}