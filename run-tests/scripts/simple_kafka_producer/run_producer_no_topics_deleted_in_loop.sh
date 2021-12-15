#!/bin/bash
brokers="10.9.228.91:9092"
zoo_keepers="10.9.228.91:2181"

# group id computed from
# "nucolumnar_aggregator." + var.variantName + "_" + var.zone + "_" + var.topic,
group_id_prefix="nucolumnar_aggregator."
variantName="kafka_cluster1_SLC_"
zone="SLC_"
topic="mib.cluster1.marketing.1"
group_id=${group_id_prefix}${variantName}${zone}${topic}
echo "group id chosen is: ${group_id}"

number_of_messages="10"
number_of_tables="1"
number_of_partitions=1

for i in $(seq 1000)
do 	 
  echo "to run launch_serializer_based_producer:  ${brokers} ${topic} ${number_of_messages} ${number_of_tables} ${number_of_partitions}"

  ../../deployed/launch_serializer_based_producer  ${brokers} ${topic} ${number_of_messages} ${number_of_tables} 

  sleep 0.5
  
done 
