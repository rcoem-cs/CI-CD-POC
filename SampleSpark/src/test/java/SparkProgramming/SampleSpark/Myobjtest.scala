package info.com.spark


import org.junit.Test

import org.apache.spark.rdd.RDD


class Myobjtest extends sparkConf{
  
  val testObj = new Func

   @Test def test1():Unit = {
     
     val expected = sc.parallelize(Array(("civil",3),("comp",2),("mech",1)))
       
     val actual = testObj.wordCount("C:/Users/saurabh.garud/Desktop/spark_input/test_input.txt")
     actual.collect().foreach(println)
     
     assert(expected.collect().mkString == actual.collect().mkString)
   
 }
   
   @Test def test2():Unit = {
     
     val expected = sc.parallelize(Array(("civil",3),("comp",2),("mech",1)))
     
     val actual =  testObj.hiveTable(testObj.wordCount("C:/Users/saurabh.garud/Desktop/spark_input/test_input.txt"))
     actual.show()
     
     assert(expected.collect().mkString == actual.collect().mkString)
     
 }
  
}