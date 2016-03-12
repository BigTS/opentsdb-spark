package org.bigts.rdd.opentsdb

import java.util
import java.util.Map.Entry

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.Array.canBuildFrom

class SparkTSDBQuery(zkQuorum: String, zkClientPort: String) extends Serializable {
  private val zookeeperQuorum = zkQuorum
  private val zookeeperClientPort = zkClientPort
  private val format_data = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm")

  /**
    * Generate RDD for the given query. All tag keys and values must exist, if tag key or value is
    * missing no data will be returned.
    *
    * @param metricName
    * @param tagsKeysValues should be on this format: tagk1->tagv1,tagk2->tagv2,....
    * @param startdate      start date of the query, should be on this format: yyyy/MM/dd HH:mm
    * @param enddate        end date of the query, should be on this format: yyyy/MM/dd HH:mm
    * @param sc             Spark Context
    * @return RDD that represents the time series fetched from opentsdb, will be on this format (time, value)
    */
  def generateRDD(
    metricName: String,
    tagsKeysValues: String,
    startdate: String,
    enddate: String,
    sc: SparkContext)
  : RDD[(Long, Float)] = {
    println("Generating RDD...")

    val tags: Map[String, String] = parseTags(tagsKeysValues)

    val tsdbUID = read_tsdbUID_table(metricName, tags, sc)
    println("tsdbUID Count: " + tsdbUID.count)

    println("MetricsUID: ")
    val metricsUID = getMetricUID(tsdbUID)
    metricsUID.foreach(arr => println(arr.mkString(", ")))

    require(!metricsUID.isEmpty, "Can't find metric: " + metricName)

    println("tagKUIDs: ")
    val tagKUIDs = getTagUIDs(tsdbUID, "tagk")
    tagKUIDs.foreach(m => println(m._1 + " => " + m._2.mkString(", ")))

    println("tagVUIDs: ")
    val tagVUIDs = getTagUIDs(tsdbUID, "tagv")
    tagVUIDs.foreach(m => println(m._1 + " => " + m._2.mkString(", ")))

    // all tags must exist
    require(!(tags.size != tagKUIDs.size || tagKUIDs.size != tagVUIDs.size),
      "Can't find keys or values")
    // TODO print missing values

    val tagKV = joinTagKeysWithValues(tags, tagKUIDs, tagVUIDs)

    val tsdb = read_tsdb_table(metricsUID, tagKV, startdate, enddate, sc)
    val timeSeriesRDD: RDD[(Long, Float)] = decode_tsdb_table(tsdb)

    timeSeriesRDD
  }

  private def decode_tsdb_table(tsdb: RDD[(ImmutableBytesWritable, Result)]): RDD[(Long, Float)] = {
    //Decoding retrieved data into RDD

    val timeSeriesRDD = tsdb
      // columns from 3-7 (the base time)
      .map(kv => (Arrays.copyOfRange(kv._1.copyBytes(), 3, 7), kv._2.getFamilyMap("t".getBytes())))
      .flatMap({
        kv =>
          val basetime: Int = ByteBuffer.wrap(kv._1).getInt
          val iterator: util.Iterator[Entry[Array[Byte], Array[Byte]]] = kv._2.entrySet().iterator()
          val row = new ArrayBuffer[(Long, Float)]

          while (iterator.hasNext()) {
            val next = iterator.next()

            val qualifierBytes: Array[Byte] = next.getKey()
            val valueBytes = next.getValue()

            // Column Quantifiers are stored as follows:
            // if num of bytes=2: 12 bits (delta timestamp value in sec) + 4 bits flag
            // if num of bytes=4: 4 bits flag(must be 1111) + 22 bits (delta timestamp value in ms) +
            //                       2 bits reserved + 4 bits flag
            // last 4 bits flag = (1 bit (int 0 | float 1) + 3 bits (length of value bytes - 1))

            // if num of bytes>4 & even: columns with compacted for the hour, with each qualifier having 2 bytes
            // TODO make sure that (qualifier only has 2 bytes)

            // if num of bytes is odd: Annotations or Other Objects

            if (qualifierBytes.length == 2) {
              // 2 bytes qualifier
              parseValues(qualifierBytes, valueBytes, basetime, 2, (x => x >> 4), row)

            } else if (qualifierBytes.length == 4 && is4BytesQualifier(qualifierBytes)) {
              // 4 bytes qualifier
              parseValues(qualifierBytes, valueBytes, basetime, 4, (x => (x << 4) >> 10), row)

            } else if (qualifierBytes.length % 2 == 0) {
              // compacted columns
              parseValues(qualifierBytes, valueBytes, basetime, 2, (x => x >> 4), row)

            } else {
              // TODO (Annotations or Other Objects)
              throw new RuntimeException("Annotations or Other Objects not supported yet")
            }
          }

          row
      })

    timeSeriesRDD
  }

  private def parseTags(tagKeyValueMap: String) =
    if (tagKeyValueMap.trim != "*") tagKeyValueMap.split(",").map(_.split("->")).map(l => (l(0).trim, l(1).trim)).toMap
    else Map[String, String]()

  private def getMetricUID(tsdbUID: RDD[(ImmutableBytesWritable, Result)]): Array[Array[Byte]] = {
    val metricUIDs: Array[Array[Byte]] = tsdbUID
      .map(l => l._2.getValue("id".getBytes(), "metrics".getBytes()))
      .filter(_ != null).collect //Since we will have only one metric uid

    metricUIDs
  }

  private def getTagUIDs(tsdbUID: RDD[(ImmutableBytesWritable, Result)], qualifier: String): Map[String, Array[Byte]] = {
    val tagkUIDs: Map[String, Array[Byte]] = tsdbUID
      .map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), qualifier.getBytes())))
      .filter(_._2 != null).collect.toMap

    tagkUIDs
  }

  private def joinTagKeysWithValues(tags: Map[String, String], tagKUIDs: Map[String, Array[Byte]],
                                    tagVUIDs: Map[String, Array[Byte]]): List[Array[Byte]] = {

    val tagkv: List[Array[Byte]] = tagKUIDs
      .map(k => (k._2, tagVUIDs(tags(k._1)))) // key(byte Array), value(byte Array)
      .map(l => (l._1 ++ l._2)) // key + value
      .toList
      .sorted(Ordering.by((_: Array[Byte]).toIterable))

    tagkv
  }

  private def parseValues(qualifierBytes: Array[Byte], valueBytes: Array[Byte], basetime: Int, step: Int,
                          getOffset: Int => Int, result: ArrayBuffer[(Long, Float)]) = {

    var index = 0
    for (i <- 0 until qualifierBytes.length by step) {
      val qualifier = parseQualifier(qualifierBytes, i, step)

      val timeOffset = getOffset(qualifier)
      val isFloat = (((qualifier >> 3) & 1) == 1)
      val valueByteLength = (qualifier & 7) + 1

      val value = parseValue(valueBytes, index, isFloat, valueByteLength)
      result += ((basetime + timeOffset, value))

      index = index + valueByteLength
    }
  }

  private def parseValue(valueBytes: Array[Byte], index: Int, isFloat: Boolean, valueByteLength: Int): Float = {
    val bufferArr = ByteBuffer.wrap(Arrays.copyOfRange(valueBytes, index, index + valueByteLength))
    if (isFloat) {
      if (valueByteLength == 4) bufferArr.getFloat()
      else if (valueByteLength == 8) bufferArr.getDouble().toFloat
      else throw new IllegalArgumentException(s"Can't parse Value (isFloat:$isFloat, valueByteLength:$valueByteLength")
    } else {
      if (valueByteLength == 1) valueBytes(index).toFloat
      else if (valueByteLength == 2) bufferArr.getShort.toFloat
      else if (valueByteLength == 4) bufferArr.getInt.toFloat
      else if (valueByteLength == 8) bufferArr.getLong.toFloat
      else throw new IllegalArgumentException(s"Can't parse Value (isFloat:$isFloat, valueByteLength:$valueByteLength")
    }
  }

  private def parseQualifier(arr: Array[Byte], startIdx: Int, endIdx: Int) = {
    val length = endIdx - startIdx
    var value = 0
    for (i <- startIdx until endIdx)
      value = value | (arr(i).toInt << ((length - 1 - i) * 8))
    value
  }

  private def is4BytesQualifier(qualifierBytes: Array[Byte]): Boolean = {
    val firstByte = qualifierBytes(0)
    ((firstByte >> 4) & 15) == 15 // first 4 bytes = 1111
  }

  private def read_tsdbUID_table(metricName: String, tags: Map[String, String], sc: SparkContext)
  : RDD[(ImmutableBytesWritable, Result)] = {

    val config = tsdbuidConfig(zookeeperQuorum, zookeeperClientPort,
      Array(metricName, tags.map(_._1).mkString("|"), tags.map(_._2).mkString("|")))

    val tsdbUID = readTable(sc, config)

    tsdbUID
  }

  private def read_tsdb_table(metricsUID: Array[Array[Byte]], tagKV: List[Array[Byte]], startdate: String,
                              enddate: String, sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {

    val config = tsdbConfig(zookeeperQuorum, zookeeperClientPort, metricsUID.last,
      if (tagKV.size != 0) Option(tagKV.flatten.toArray) else None,
      if (startdate != "*") Option(startdate) else None,
      if (enddate != "*") Option(enddate) else None
    )

    val tsdb = readTable(sc, config)
    tsdb
  }

  def readTable(sc: SparkContext, config: Configuration) = {
    sc.newAPIHadoopRDD(
      config,
      classOf[TSDBInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
  }

  //Prepares the configuration for querying the TSDB-UID table and extracting UIDs for metrics and tags
  private def tsdbuidConfig(zookeeperQuorum: String, zookeeperClientPort: String, columnQ: Array[String]) = {
    val config = generalConfig(zookeeperQuorum, zookeeperClientPort)
    config.set(TableInputFormat.INPUT_TABLE, "tsdb-uid")
    config.set(TSDBScan.TSDB_UIDS, columnQ.mkString("|"))
    config
  }

  //Prepares the configuration for querying the TSDB table
  private def tsdbConfig(zookeeperQuorum: String, zookeeperClientPort: String,
                         metricUID: Array[Byte], tagkv: Option[Array[Byte]] = None,
                         startdate: Option[String] = None, enddate: Option[String] = None): Configuration = {

    val config = generalConfig(zookeeperQuorum, zookeeperClientPort)
    config.set(TableInputFormat.INPUT_TABLE, "tsdb")

    config.set(TSDBScan.METRICS, bytes2hex(metricUID))
    if (tagkv != None) {
      config.set(TSDBScan.TAGKV, bytes2hex(tagkv.get))
    }

    if (startdate != None)
      config.set(TSDBScan.SCAN_TIMERANGE_START, getTime(startdate.get))

    if (enddate != None)
      config.set(TSDBScan.SCAN_TIMERANGE_END, getTime(enddate.get))

    config
  }

  private def generalConfig(zookeeperQuorum: String, zookeeperClientPort: String): Configuration = {
    //Create configuration
    val config = HBaseConfiguration.create()
    //config.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml")
    config.set("hbase.zookeeper.quorum", zookeeperQuorum)
    config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
    config.set("hbase.mapreduce.scan.column.family", "t")
    config
  }

  private def getTime(date: String): String = {
    val MAX_TIMESPAN = 3600
    def getBaseTime(date: String): Int = {
      val timestamp = format_data.parse(date).getTime()
      val base_time = ((timestamp / 1000) - ((timestamp / 1000) % MAX_TIMESPAN)).toInt
      base_time
    }

    val baseTime = getBaseTime(date)
    val intByteArray: Array[Byte] = ByteBuffer.allocate(Integer.BYTES).putInt(baseTime).array()
    bytes2hex(intByteArray)
  }

  //Converts Bytes to Hex (see: https://gist.github.com/tmyymmt/3727124)
  private def bytes2hex(bytes: Array[Byte]): String = {
    val sep = "\\x"
    val regex = sep + bytes.map("%02x".format(_).toUpperCase()).mkString(sep)
    regex
  }

}
