package com.Infocepts.UnitTesting

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class UnitTestQA extends FunSuite with BeforeAndAfterAll{
  test("Count Words"){

    val sparkAppConf = new SparkConf()
    sparkAppConf.set("spark.app.name","WordCount Program")
    sparkAppConf.set("spark.master","local[3]")
    sparkAppConf.set("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083")


    val spark = SparkSession.builder()
      .config(sparkAppConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val text = spark.read.textFile("input.txt")
    val cnt = text.flatMap(line => line.split(" ")).count.asInstanceOf[Int]
    val df = spark.sql("select wordcount from cicdpoc.wordcount_dev").toDF
    val num = df.rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)

    println("cnt->"+cnt)
    println("num->"+num)

    assert(cnt == num)

    spark.stop()
  }
}
