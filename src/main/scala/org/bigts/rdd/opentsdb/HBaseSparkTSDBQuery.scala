package org.bigts.rdd.opentsdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext

class HBaseSparkTSDBQuery(zkQuorum: String, zkClientPort: String)
  extends SparkTSDBQuery(zkQuorum, zkClientPort) {

  override def readTable(sc: SparkContext, config: Configuration) = {
    val hbaseContext = new HBaseContext(sc, config)
    val tableName = config.get(TableInputFormat.INPUT_TABLE)
    val scan = TSDBScan.createScan(config)
    hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
  }
}
