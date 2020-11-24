package com.Infocepts.POC

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DevCode {
  def main(args: Array[String]): Unit = {
    val sparkAppConf = new SparkConf()
    sparkAppConf.set("spark.app.name","WordCount Program")
    sparkAppConf.set("spark.master","local[3]")
    sparkAppConf.set("spark.sql.debug.maxToStringFields","1000")
    sparkAppConf.set("spark.sql.shuffle.partitions","2")
    sparkAppConf.set("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083")


    val spark = SparkSession.builder()
      .config(sparkAppConf)
      .enableHiveSupport()
      .getOrCreate()
    val inputFile = spark.read.textFile("/user/cicd/input.txt")

    val counts = wrdCount(spark,inputFile)

    val CountsDF = counts.withColumn("CreationTimeStamp",current_timestamp())

    spark.sql("create database if not exists cicdpoc").show()
    CountsDF.createOrReplaceTempView("wordcount")
    spark.sql("drop table if exists cicdpoc.wordcount_"+ args(0))
    spark.sql("create table cicdpoc.wordcount_"+ args(0) + " as select * from wordcount").show()
    val df = spark.sql("select wordcount from cicdpoc.wordcount_dev").toDF
    df.show()

    spark.stop()
  }

  def wrdCount(spark:SparkSession,inputFile:Dataset[String]):DataFrame = {
    import spark.implicits._
    inputFile.flatMap(line => line.split(" ")).map(word => (word,1)).rdd.reduceByKey(_+_).toDF("word","wordcount")
  }
}
