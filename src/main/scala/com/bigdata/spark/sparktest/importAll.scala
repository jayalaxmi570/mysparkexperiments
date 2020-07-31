package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object importAll {
//MS sql
  val msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb"
  val msprop = new java.util.Properties()
  msprop.setProperty("user","msuername")
  msprop.setProperty("password","mspassword")
  msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  //Mysql
  val myurl = "jdbc:mysql://mysql.ckvslohv4g42.us-east-1.rds.amazonaws.com:3306/mysqldb"
  val myprop = new java.util.Properties()
  myprop.setProperty("user","musername")
  myprop.setProperty("password","mpassword")
  myprop.setProperty("driver","com.mysql.cj.jdbc.Driver")

  //Oracle details
  val ourl = "jdbc:oracle:thin:@//corona2020.cngsgxxckbvf.us-east-2.rds.amazonaws.com:1521/ORCL"
  val oprop=new java.util.Properties()
  oprop.setProperty("user","ousername")
  oprop.setProperty("password","opassword")
  oprop.setProperty("driver","oracle.jdbc.OracleDriver")



  }
