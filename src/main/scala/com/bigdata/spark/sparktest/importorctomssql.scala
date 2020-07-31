package com.bigdata.spark.sparktest

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparktest.importAll._

object importorctomssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("importorctomssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //val odf=spark.read.jdbc(ourl,"emp",oprop)
    //two tables data reading
    /*val tabs=Array("emp","dept")
      tabs.foreach{
      x=>
        val df=spark.read.jdbc(ourl,s"$x",oprop)
        df.show()
    }*/
    //All tables data reading
    /*val query="(select table_name from all_tables where tablespace_name='users') t "
    val tabs=spark.read.jdbc(ourl,query,oprop).rdd.map(x=>x(0)).collect.toList
    tabs.foreach{
      x=>val df=spark.read.jdbc(ourl,s"$x",oprop)
        df.show()
        //df.write.mode(SaveMode.Overwrite).jdbc(msurl,s"$x"+"jaya",msprop)
        df.write.jdbc(myurl,s"$x"+"jayaj",myprop)

    }




    spark.stop()
  }
}*/
    val query = "(select TABLE_NAME from all_tables WHERE TABLESPACE_NAME='USERS') t"
    val tabs = spark.read.jdbc(ourl,query,oprop).rdd.map(x=>x(0)).collect.toList.filter(x=>x!="EMP")

    tabs.foreach{x=>
      val df1 = spark.read.jdbc(ourl,s"$x",oprop)
      df1.show()
      df1.write.jdbc(msurl,s"$x"+"jayaj",msprop)
    }

    spark.stop()
  }
}