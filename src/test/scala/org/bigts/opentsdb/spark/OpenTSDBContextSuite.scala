package org.bigts.opentsdb.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{ShouldMatchers, FunSuite}

class OpenTSDBContextSuite extends FunSuite with ShouldMatchers {

  def createSC(): SparkContext = {
    new SparkContext(new SparkConf()
      .setAppName("OpenTSDB-Spark")
      .setMaster("local"))
  }

  def testCase(metric: String, tagVal: String, startD: String, endD: String,
               verify: (RDD[(Long, Float)]) => Unit): Unit = {
    val sc = createSC()

    val config = HBaseConfiguration.create()

    val opentsdb = new OpenTSDBContext(sc, config)

    val rdd: RDD[(Long, Float)] = opentsdb.generateRDD(
      metricName = metric,
      tagsKeysValues = tagVal,
      startDate = startD,
      endDate = endD)

    printRDD(rdd)

    verify(rdd)

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

  def printRDD(rdd: RDD[(Long, Float)]) = {
    //Total number of points
    println("TimeSeries Data Count: " + rdd.count)

    //Collect & Print the data
    rdd.collect.foreach(point => println(point._1 + ", " + point._2))
  }
}
