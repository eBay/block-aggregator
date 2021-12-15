#!/bin/bash 

#unlike at the kafka server/zookeeper server start up, both shutdown needs to be done synchronously, as otherwise,
#kafka-server will still complain no zookeeper server found.

##to have kafka-server stopped first and after, zookeeper server, as otherwise, kafka-server will complain no zookeeper
##to connect to. And afterwards, kafka-server can not be terminated successfully.
$KAFKAHOME/bin/kafka-server-stop.sh  
sleep 10

$KAFKAHOME/bin/zookeeper-server-stop.sh  
sleep 10
