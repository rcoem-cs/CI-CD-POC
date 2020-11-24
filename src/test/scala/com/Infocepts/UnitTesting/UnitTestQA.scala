package com.Infocepts.UnitTesting

import com.Infocepts.POC.DevCode.wrdCount
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.parallel.mutable

class UnitTestQA extends FunSuite with BeforeAndAfterAll{
  test("Count Words"){
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




    assert(wordMap("Butter") == 20,"Count for word -> Butter should be 2")






  }
}
