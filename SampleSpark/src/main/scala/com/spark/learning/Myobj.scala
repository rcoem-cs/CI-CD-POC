package info.com.spark

import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Range._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._



object Myobj {
  def main(args:Array[String])
  { 
 
   // val path = "C:\\Users\\saurabh.garud\\Desktop\\spark_input\\normal_text.txt"
    val path = args(0)
    val funObj = new Func()
    
    funObj.hiveTable(funObj.wordCount(path)).show()
  
  
  }   
}