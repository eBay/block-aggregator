#!/bin/bash 

$KAFKAHOME/bin/zookeeper-server-start.sh $KAFKAHOME/config/zookeeper.properties &
#To have enough time to allow zookeeper to be started, before registration of kafka-server.
sleep 10

$KAFKAHOME/bin/kafka-server-start.sh $KAFKAHOME/config/server.properties & 
sleep 10
