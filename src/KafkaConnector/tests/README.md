# List of Tests

## `kafka_connector_tests.cpp`
It tests the functionality of the  `KafkaConnector` class. The tests are in `kafka_connector_tests.cpp`. We test following test functions for different number of tables and partitions.

| Test function | Explanation |
|---------------|-------------|
| happy_path               | It produce a set of messages to the Kafka. Then, it creates an object of the `KafkaConnector` and then give some time to it to consume the data and write it to a file. It then checks the file to make sure everything has been consumed sucessfully.              |
| replay_test              | It basically first run the the happy_path first. Then, it stops the `KafkaConnector`. Then, it produce more, and run the `KafkaConnector` after producer is done. At the end, it makes sure `KafkaConnector` has consumed all messages.              | 
| replay_test_with_storage_failure              | It simulate when the backend store has failed and `blockWait` function returns failure. It then recover the storage and at the end expect to see all messages have been consumed and written to the storage.             |
| partition_reassignment_test | This test call a producer to write to Kafka constantly. It then create a `KafkaConnector` object to read from the kafka. After some time, creates the second `KafkaConnector` object. After some time, it stops the first `KafkaConnector`. It then start it again, and wait for producer to finish. It then wait some additional time, and finally check the message to make sure everything is consumed.         |

During all of the tests above, the `InvariantChecker` makes sures that now overlapping batches are ever flushed. Flushing any overlapping batches cause an assertation failure. 

## `metadata_tests.cpp`
It tests the `Metadata` class.  

| Test function | Explanation |
|---------------|-------------|
| basic_test    | It add some metadata for some tables to the object, and then ask object for the min/max. It also test serialization and desrialization. 

## `buffer_tests.cpp`
This tests `SimpleBuffer` class. 

| Test function | Explanation |
|---------------|-------------|
| basic_test    | It test the `flushable` check of the `SimpleBuffer` class for both time and batch size trigers. |
