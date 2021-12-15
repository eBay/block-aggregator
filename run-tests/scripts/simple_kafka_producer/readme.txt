This is a simple integration testing with Aggregator and A single-node Kafka Cluster.

NOTE: Step 2 only invokes for the first time that the topic is registerred to Kafka Cluster. After the first time,
      only Step 1, and then Step 3 should run.


(1) Launch the aggregator at the repo top directory:
    (1.1) cp ./run-tests/conf/example_aggregator_config.json  /tmp/.
    (1.2) cd cmake-build-release;
    (1.3) ./NuColumnarAggr --config-file /tmp/example_aggregator_config.json

after step 1.3, we can go to "logs" directory under cmake-build-release, and use "tail -f NuColumnarAggr.INFO" to monitor the progress.
It turns out that on Linux workstation, on /tmp/example_aggregator_config.json, we need to have the following glog setting to be changed
from the original setting in the file:

"FLAGS_logtostderr": 0

So that all console logs go to the INFO level file logging


To retrieve the metrics, use:

curl -k https://localhost:13008/metrics


Before going to Step 2, make sure that "kafka-topics.sh" is on the path, for example, /opt/kafka_2.11-2.1.1/bin/kafka-topics.sh

Note that on Tess130, the unit testing server has already launched the kafka server. Even though we have /tmp/example_aggregator_config.json to
specify the IP address, we still experience that the pod name can not be resolved for the IP addres. As a result, we need to add the following 
on the local workstation in /etc/hosts:

"
# To add tess130 broker for kafka testing
10.9.228.91     nucolumnaraggr-unittests-server-78fd7b868d-msn7v

"

(2) Run:
    ./run_producer.sh

NOTE, if the CH server is in C3, then go to the CH server "nucolumnar-1" in C3 (with IP address: 10.194.224.19, named nucolumnar-1), 
launch "clickhouse-client".Then in the CLI command shell, issue: select count(*) from table: simple_event_5;

Then we should be able to see the number of the rows increased that matches: number_of_messages *3, 

If the CH server is in Tess130, we can find the unit test server pod name, and then get into the pod and launch "clickhouse-client".
    
(3) Run:
    ./run_producer_no_topics_deleted.sh

then we can see the rows gets further increased that matches: number_of_messages *3

The difference between "run_producer_no_topics_deleted.sh" and "run_producer.sh" is that "run_producer.sh" creates the topic, 
but "run_producer_no_topics_deleted.sh" does not create new topic. It just inherits the topic created from "run_producer.sh"


(4) to repeatedly run the comand in step 3, we can use the scripts in the same directory: run_producer_no_topics_deleted_in_loop.sh,
to continously run the scripts to inject data to the Clickhouse Server. 

(5) Note that run_producer-other.sh and run_producer_no_topics_deleted-other.sh currently use the same topic as run-producer*.sh.
To introduce a new topic, we will need to change  /tmp/example_aggregator_config.json to have two topics subscribed by the lanched
aggregator at Step 1. Currently, only one topic is subscribed.


