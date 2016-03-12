package org.bigts.rdd.opentsdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class HBaseSparkTSDBQuerySuite extends FunSuite with ShouldMatchers {

  val zookeeperQuorum = "localhost"
  val zookeeperClientPort = "2181"

  def createSC(): SparkContext = {
    new SparkContext(new SparkConf()
      .setAppName("OpenTSDB-Spark")
      .setMaster("local"))
  }

  def testCase(metric: String, tagVal: String, startD: String, endD: String,
                  verify: (RDD[(Long, Float)]) => Unit): Unit = {
    val sc = createSC()

    val hbaseSpark = new HBaseSparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)

    val dataHBaseSpark: RDD[(Long, Float)] = hbaseSpark.generateRDD(
      metricName = metric,
      tagsKeysValues = tagVal,
      startdate = startD,
      enddate = endD,
      sc)

    printRDD(dataHBaseSpark, "New API")

    verify(dataHBaseSpark)

    sc.stop()
  }

  test("One tag") {
    testCase(
      "product.sales",
      "id->1",
      "2016/01/01 00:00",
      "2016/02/29 10:00",
      rdd => {
        rdd.count() should be (60)
      })
  }

  test("Two tags") {
    testCase(
      "product.sales",
      "id->1,store->1",
      "2016/01/01 00:00",
      "2016/02/29 10:00",
      rdd => {
        rdd.count() should be (60)
      })
  }

  def printRDD(rdd: RDD[(Long, Float)], prefix: String) = {
    //Total number of points
    println(s"$prefix: TimeSeries Data Count: " + rdd.count)

    //Collect & Print the data
    rdd.collect.foreach(point => println(s"$prefix: " + point._1 + ", " + point._2))
  }
}
