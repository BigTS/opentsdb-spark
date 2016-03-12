package org.bigts.rdd.opentsdb

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{ShouldMatchers, FunSuite}

class TestSuite extends FunSuite with ShouldMatchers {

  def createSC(): SparkContext = {
    new SparkContext(new SparkConf()
      .setAppName("OpenTSDB-Spark")
      .setMaster("local"))
  }

  test("Old API") {
    val zookeeperQuorum = "localhost"
    val zookeeperClientPort = "2181"
    val metric = "product.sales"
    val tagVal = "id->1,store->1"
    val startD = "2016/01/01 00:00"
    val endD = "2016/02/29 10:00"

    val sc = createSC()

    //Connection to OpenTSDB
    val sparkTSDB = new SparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)

    //Create RDD from OpenTSDB
    val dataOldAPI: RDD[(Long, Float)] = sparkTSDB.generateRDD(
      metricName = metric,
      tagsKeysValues = tagVal,
      startdate = startD,
      enddate = endD,
      sc)

    printRDD(dataOldAPI, "Old API")

    dataOldAPI.count() should be (60)

    sc.stop
  }

  test("New API") {
    val zookeeperQuorum = "localhost"
    val zookeeperClientPort = "2181"
    val metric = "product.sales"
    val tagVal = "id->1"
    val startD = "2016/01/01 00:00"
    val endD = "2016/02/29 10:00"

    val sc = createSC()

    val hbaseSpark = new HBaseSparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)

    val dataHBaseSpark: RDD[(Long, Float)] = hbaseSpark.generateRDD(
      metricName = metric,
      tagsKeysValues = tagVal,
      startdate = startD,
      enddate = endD,
      sc)

    printRDD(dataHBaseSpark, "New API")

    dataHBaseSpark.count() should be (60)

    sc.stop
  }

  def printRDD(rdd: RDD[(Long, Float)], prefix: String) = {
    //Total number of points
    println(s"$prefix: TimeSeries Data Count: " + rdd.count)

    //Collect & Print the data
    rdd.collect.foreach(point => println(s"$prefix: " + point._1 + ", " + point._2))
  }
}
