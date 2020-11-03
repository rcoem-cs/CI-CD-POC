package com.Infocepts.QATesting

import org.junit.Test
import junit.framework.TestCase
import junit.framework.TestSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

class QATest extends sparkConf{
  
    @Test 
    def test1():Unit = {
     
     val text = spark.read.textFile("input.txt")

     import spark.implicits._
     val cnt = text.flatMap(line => line.split(" ")).count.asInstanceOf[Int]
     println("cnt value =",cnt)
     val df = spark.sql("select wordcount from cicdpoc.wordcount_dev").toDF
     val num = df.rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
     println("num value = ",num) 
     assert(cnt == num)
     
     spark.stop()
   
 }
  
}