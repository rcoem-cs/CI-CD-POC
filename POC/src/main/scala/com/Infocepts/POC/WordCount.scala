package com.Infocepts.POC

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) : Unit = {
      val spark = SparkSession.builder().appName("CICD Pipeline POC").config("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083").enableHiveSupport().getOrCreate()
      
      val text = spark.read.textFile("/user/cicd/input.txt")

      import spark.implicits._  
      val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).rdd.reduceByKey(_+_).toDF("word","wordcount")
      
      spark.sql("create database if not exists cicdpoc").show()
      counts.createOrReplaceTempView("wordcount")
      spark.sql("drop table if exists cicdpoc.wordcount_"+ args(0))
      spark.sql("create table cicdpoc.wordcount_"+ args(0) + " as select * from wordcount").show()
      val df = spark.sql("select wordcount from cicdpoc.wordcount_dev").toDF
      df.show()
      
      spark.stop()
      
  }
}