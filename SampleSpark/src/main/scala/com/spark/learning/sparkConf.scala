package info.com.spark

  import org.apache.spark.sql.SparkSession
import org.apache.hive._
import org.apache.spark.sql.hive._

class sparkConf {
  
 // val spark = SparkSession.builder().master("local").enableHiveSupport().appName("Myapp").getOrCreate()
              
    val spark = SparkSession.builder().config("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083").enableHiveSupport().appName("Myapp").getOrCreate()         
  
    val sc = spark.sparkContext
}
