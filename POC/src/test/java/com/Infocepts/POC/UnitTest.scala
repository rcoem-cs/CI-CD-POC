package com.Infocepts.POC

import org.junit.Test
import com.Infocepts.POC.WordCount.wrdCount
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.mutable

class UnitTest {
  @Test def splitText():Unit = {
    val sparkAppConf = new SparkConf()
    sparkAppConf.set("spark.app.name","Unit test cases")
    sparkAppConf.set("spark.master","local[3]")

    val spark = SparkSession.builder()
      .config(sparkAppConf)
      .getOrCreate()

    val inputFile = spark.read.textFile("input.txt")

    val result = wrdCount(spark,inputFile)

    val wordMap = new mutable.ParHashMap[String,Integer]
    result.collect().foreach(r => wordMap.put(r.getString(0),r.getInt(1)))

    assert(wordMap("Butter") == 2,"Count for word -> Butter should be 2")

    spark.stop()
  }
}