package org.bigts.rdd.opentsdb

import org.apache.spark.rdd.RDD
import org.scalatest.{ShouldMatchers, FunSuite}

class TestSuite extends FunSuite with ShouldMatchers {
  test("Main") {
    val sparkMaster = "local"
    val zookeeperQuorum = "localhost"
    val zookeeperClientPort = "2181"
    val metric = "product.sales"
    val tagVal = "id->1"
    val startD = "2016/01/01 10:00"
    val endD = "2016/02/29 10:00"

    val sc = CustomSparkContext.create(sparkMaster = sparkMaster, zookeeperQuorum = zookeeperQuorum,
      zookeeperClientPort = zookeeperClientPort)

    //Connection to OpenTSDB
    val sparkTSDB = new SparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)

    //Create RDD from OpenTSDB
    val dateOldAPI: RDD[(Long, Float)] =
      sparkTSDB.generateRDD(metricName = metric, tagsKeysValues = tagVal, startdate = startD, enddate = endD, sc)

    val hbaseSpark = new HBaseSparkTSDBQuery(zookeeperQuorum, zookeeperClientPort)
    val dataHBaseSpark: RDD[(Long, Float)] =
      hbaseSpark.generateRDD(metricName = metric, tagsKeysValues = tagVal, startdate = startD, enddate = endD, sc)

    printRDD(dateOldAPI, "Old API")
    printRDD(dataHBaseSpark, "New API")

    sc.stop
  }

  def printRDD(rdd: RDD[(Long, Float)], prefix: String) = {
    //Total number of points
    println(s"$prefix: TimeSeries Data Count: " + rdd.count)

    //Collect & Print the data
    rdd.collect.foreach(point => println(s"$prefix: " + point._1 + ", " + point._2))
  }
}
