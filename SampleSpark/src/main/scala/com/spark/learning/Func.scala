package info.com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class Func extends sparkConf{
  
     def wordCount(url:String):RDD[(String,Int)] = {
      
      val wc = sc.textFile(url)
               .flatMap(row => row.split(" "))
               .map(word => (word,1))
               .reduceByKey(_+_)
      wc
      }
     
     def hiveTable(wc:RDD[(String,Int)]):DataFrame =  {
      
      import spark.implicits._
      val wcDf = wc.toDF("Word","Count")
      wcDf.createOrReplaceTempView("wordcount_table")
      spark.sql("create database if not exists cicdDB")
      spark.sql("use cicdDB")
      spark.sql("create table if not exists cicdDB.wordCount as select * from wordcount_table")
      val wcDF = spark.sql("select * from cicdDB.wordCount")
     // spark.sql("drop table cicdDB.wordCount")
      wcDF
    }
  
}