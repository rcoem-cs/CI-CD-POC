package com.Infocepts.QATesting

import org.apache.spark.sql.SparkSession

class sparkConf {
  val spark = SparkSession.builder().master("local").appName("CICD Pipeline POC QA").config("hive.metastore.uris","thrift://im-hdp-mgr1.infocepts.com:9083").enableHiveSupport().getOrCreate()
}