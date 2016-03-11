#!/bin/bash

### Hadoop
#wget https://archive.apache.org/dist/hadoop/common/stable/hadoop-2.7.2.tar.gz
#tar -xzf hadoop-2.7.2.tar.gz

### Zookeeper
#wget http://www-us.apache.org/dist/zookeeper/stable/zookeeper-3.4.8.tar.gz
#tar -xzf zookeeper-3.4.8.tar.gz
#cp zookeeper-3.4.8/conf/zoo_sample.cfg zookeeper-3.4.8/conf/zoo.cfg
#zookeeper-3.4.8/bin/zkServer.sh start

## Hbase
echo Downloading hbase ...
wget http://www-us.apache.org/dist/hbase/stable/hbase-1.1.3-bin.tar.gz
echo untaring hbase ...
tar -xzf hbase-1.1.3-bin.tar.gz
echo starting hbase ...
./hbase-1.1.3/bin/start-hbase.sh
echo jps ...
jps

## OpenTSDB
echo Downloading OpenTSDB ...
wget https://github.com/OpenTSDB/opentsdb/releases/download/v2.2.0/opentsdb-2.2.0_all.deb
echo Installing OpenTSDB ...
sudo dpkg -i opentsdb-2.2.0_all.deb
echo Creating OpenTSDB tables ...
sudo env COMPRESSION=NONE HBASE_HOME=$(pwd)/hbase-1.1.3 /usr/share/opentsdb/tools/create_table.sh
echo Starting OpenTSDB ...
sudo service opentsdb start
echo Creating metric product.sales ...
sudo /usr/share/opentsdb/bin/tsdb mkmetric product.sales
echo Loading data into metric product.sales
sudo /usr/share/opentsdb/bin/tsdb import src/test/resources/data.txt

