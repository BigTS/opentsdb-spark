opentsdb-spark
==============

Module for accessing OpenTSDB data through Spark.

#Installation.
##On your client (SparkDriver)
  >Execute the following in your terminal
  1. wget https://github.com/BigTS/opentsdb-spark/archive/master.zip
  2. unzip master.zip
  3. cd opentsdb-spark-master
  4. sbt eclipse (if you dont have sbt, it can be installed from www.scala-sbt.org)
  5. The project can be now imported into eclipse

##On your cluster.
  >For each node in the cluster add the following into your com file
  1. nano $Spark_Home/conf/spark-env.sh
  2. copy the following to the end of the file.
  
    export HBASE_HOME=/path/to/your/hbase/dir

    for dir in $HBASE_HOME/lib/*.jar
    do
      if [ "$dir" = "$HBASE_HOME/lib/netty-3.2.4.Final.jar" ] ; then
        continue;
      fi

      if [ "$dir" = "$HBASE_HOME/lib/netty-all-4.0.23.Final.jar" ] ; then
        continue;
      fi

    SPARK_CLASSPATH="$SPARK_CLASSPATH:$dir"
    done

#System Info
  Apache Hadoop 2.6.0, Apache Hbase 1.1.2, Apache Spark 1.6.0, Scala 2.10.5



