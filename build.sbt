name := "opentsdb-spark"

version := "0.2"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet")