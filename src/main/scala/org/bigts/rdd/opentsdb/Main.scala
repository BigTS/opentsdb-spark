/**
  * Test Access to OpenTSDB
  */
package org.bigts.rdd.opentsdb

import org.apache.spark.rdd.RDD


object Main {
  def main(args: Array[String]) {

    //    if(args.length != 9) {
    //      println("Required params.: sparkmaster zkqourum zkport metric tagkeyval startdate enddate driverhost driverport")
    //      System.exit(1)
    //    }
    //
    //    val sparkMaster = args(0) //"spark://ip.or.hostname:port" //"local (for localhost)"
    //    val zookeeperQuorum = args(1) //"ip.or.hostname"
    //    val zookeeperClientPort = args(2) //"zookeeper port"
    //    val metric = args(3) //"Metric.Name"
    //    val tagVal = args(4) //"tag.key->tag.value" (can also be * or tag.key->*)
    //    val startD = args(5) //"ddmmyyyyhh:mm" (or can be *)
    //    val endD = args(6) //"ddmmyyyyhh:mm" (or can be)

    val sparkMaster = "local"
    val zookeeperQuorum = "localhost"
    val zookeeperClientPort = "2181"
    val metric = "bigts.size"
    val tagVal = "age->24,status->engaged"
    val startD = "2010/02/22 10:00"
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