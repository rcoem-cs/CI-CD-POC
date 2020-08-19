package com.spark.learning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Demo {
  def main(args: Array[String])  
    {
        val conf = new SparkConf()
        conf.set("spark.master", "local")
        conf.set("spark.app.name", "DempSparkApp")
        val sc = new SparkContext(conf)
        // prints Hello World 
        println("Welcome to GIT")
        println("Welcome to CI/CD POC")
        println("Welcome to the world of DevOps world")
        println("Welcome back !!!")
        println("sab bakchodi hai !!!!")
        sc.stop()
    } 
}
