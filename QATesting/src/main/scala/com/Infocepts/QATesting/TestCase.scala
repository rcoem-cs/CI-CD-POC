package com.Infocepts.QATesting

import org.apache.spark.sql.SparkSession

object TestCase {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().appName("CICD Pipeline POC").config("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083").enableHiveSupport().getOrCreate()

    val text = spark.read.textFile("/user/cicd/input.txt")
    import spark.implicits._
    val cnt = text.flatMap(line => line.split(" ")).count.asInstanceOf[Int]
    val df = spark.sql("select wordcount from cicdpoc.wordcount_dev").toDF
    val num = df.rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    if (cnt == num){
      println("Test case has passed !!!!")
    }
    else{
      println("Test case has failed !!!!")
    }

  }
}
