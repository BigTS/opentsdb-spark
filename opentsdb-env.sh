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
wget http://www-us.apache.org/dist/hbase/stable/hbase-1.1.3-bin.tar.gz
tar -xzf hbase-1.1.3-bin.tar.gz
./hbase-1.1.3/bin/start-hbase.sh


## OpenTSDB
wget https://github.com/OpenTSDB/opentsdb/releases/download/v2.2.0/opentsdb-2.2.0_all.deb
sudo dpkg -i opentsdb-2.2.0_all.deb
env COMPRESSION=NONE HBASE_HOME=$(pwd)/hbase-1.1.3 /usr/share/opentsdb/tools/create_table.sh
sudo service opentsdb start
sudo /usr/share/opentsdb/bin/tsdb mkmetric product.sales
sudo /usr/share/opentsdb/bin/tsdb import src/test/resources/data.txt

