# List of Tests

## `test_aggregator_loader.cpp`
It tests the functionality of the  `AggregatorLoader` class.
 
| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------|
| initConnectionToChServer    | to test connection to the local ClickHouse server     | + |
| InsertARowToCHServerWithInsertQuery             | to issue the insert query statement and load a single row in the query to the local ClickHouse server            |  + | 
| InsertARowToCHServerWithDirectBlock2             | to issue the insert query statement annd  a blcok to the local ClickHou server         | + | 
| InsertARowToCHServerWithDefaultValuesInSingleBlock | to construct the block that contains default values, and insert the block to the local ClickHouse server         | + | 


## `test_aggregator_loader_manager.cpp`
It tests the `AggregatorLoaderManager` class.  

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------| 
| testInitializeLoaderConnection    | to test the initialization of the connection to the local ClickHouse server | ++ | 
| testInitDefinedTables    | to test the retrieval of all of the defined table names in the "default" database of the local ClickHouse server | ++ | 
| testRetrieveAllTableDefinitions    | to retrieve all of the table definitions for all of the tables defined in the "default" database of the local ClickHouse server | ++ | 
| testRetrieveTableDefinitionWithoutDefaults    | to retrieve a particular table definition that does not contain the `Default Value` definitions |  ++ | 
|testRetrieveTableDefinitionWithDefaults    | to retrieve a particular table definition that contains the `Default Value` definitions |  ++ |  

## `test_aggregator_tabledefinitions.cpp`
It tests `AggregatorLoader` class for all of the different kinds of table queries. 

| Test function | Explanation | Need Stronger Assertion|
|---------------|-------------| -----------------------|
| retrieveTableDefinitionViaTableQuery   | to retrieve a particular table definition, from the table name provided from the configuration file | ++ | 


## `test_blocks.cpp`
It tests how an in-memory block is constructed based on the column-based definition (column names/columns types) 

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------|
| testBlockConstructionWithExplicitTableSchema2   | to contruct a block that contains the table (columns) defintions that have `String` type and `UInt64` type . | ++ | 

## `test_columns.cpp`
It tests how in-memory columns are constructed to form the block. 

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| -----------------------|
| testColumnsWithColumnTypeStringConstruction   | to construct a single column that has the `String` type |  no need
| testColumnsWithColumnTypeUInt64Construction  | to construct a single column that has the `UInt64` type |  no need 
| testColumnsWithColumnTypeConstruction | to construct all of the supported column data types |  no need


## `test_configuraiton_loading.cpp`
It tests how the flat-buffer based nucolumnar configuration file gets loaded and examined for the loaded attributes. 

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------| 
| testDefaultConstantInConfigurationFile   | to test the retrieval of the attributes in the configuration that has the default valeus | no need | 
| loadingOfConfigurationFile  | to test the loading of the whole configuration files and selectively check the loaded attributes | no need | 

## `test_connection_pooling.cpp`
It tests how the connection pooling works in the `AggregatorLoaderManager` class and the "AggregatorLoader` class.
 

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------| 
| testConsumeAllConnectionPoolEntriesInSequentialMode   | to test how the sequential function uses up all of the connected entries up to the maximum number of the entries allowed from the configuraiton. Afterwards, the thread will be block as the connection entries run out.| ++ |
|  testConsumePoolEntriesInConcurrentMode | to run multiple woker threads and each worker thread invokes the connection to the local ClickHouse server. Ensure that the connected entries created in the pool will not run out. | ++ | 

## `test_defaults_serializer_loader.cpp`
It tests the `Aggregator Loader` class works with the protobuf de-serializer to construct the blocks that contain columns with `default value` table column definition. 
 

| Test function | Explanation | Need Stronger Assertion| 
|---------------|-------------|------------------------| 
| testASTTreeParsingForDefaultExpression  | to test how the `Default Expression` work in the ClickHouse parser.| ++ | 
|  testGetDefaultsFromTableDefinition | to test out how the `Default Expression ` and Columns withe default expressions are combined into a single data structure that is then stored in a map, with column naming being the key.| ++ | 
| InsertARowWithDefaultRowMissingAndWithDefaultConstant  | to test insertion of the block with a row that is declared with `Default Value`, and the default value is with a default constant.| ++ | 
| InsertARowWithDefaultRowMissingAndDefaultEvalFunction  | to test insertion of the block with a row that is declared with `Default Value`, and the default value is with an expression `today()`.| ++ | 
| InsertARowWithDefaultRowNoMissing | to test insertion of the block with a row that is declared with `Default Value`, but the row has been assigned with some value to override the default values.| ++ | 
| InsertARowWithDefaultRowNoMissingMultiBatches | to test insertion of the block with a row that is declared with `Default Value`, but the row has been assigned with some value to override the default values. Multiple rows are inserted as a batch to form a block| ++ | 
| InsertThreeRowsMixedWithDefaultRowNoMissingAndOtherMissing | to test the insertion of two rows: one have default values missing, and the other has default values populated. two rows get consolidated with one block.| ++ | 

## `test_ioservice_threadpool.cpp`
It tests the `IOServiceThreadPool` class that is constructed from the Boost IO_Service framework. 

| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------|
| testLaunchingOneThreadAndManyTasks  | to test how the `IOServiceThreadPool` work with one worker thread but with many tasks to be launched.| no need | 
|  testLaunchingTwoThreadsAndManyTasks | to test how the `IOServiceThreadPool` work with two worker threads but with many tasks to be launched| no need | 


## `test_lowcardinality_serializer.cpp`

It tests how the `low cardinality` defined columns are constructed in the block and then gets sent to the local ClickHouse server.


| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------|
| InsertARowWithLowCardinalityString | to test the construction of the block that contains the column with definition of `low cardinality (String)` | ++ | 
| InsertMultipleRowsWithLowCardinalityStringValues | to test the construction of the block that contains the column with definition of `low cardinality (String)` with multipe rows in a batch| ++ | 
| InsertMultipleRowsWithLowCardinalityStringValues | to test the construction of the block that contains the column with definition of `low cardinality (String)` with multipe rows in a batch| ++ |

## `test_nullable_serializer.cpp`

It tests how the `nullable (type)` defined columns are constructed in the block and then gets sent to the local ClickHouse server.


| Test function | Explanation | Need Strong Assertion |
|---------------|-------------| ----------------------|
| InsertARowWithNullableString | to test the construction of the block that contains the column with definition of `nullable (String)` | ++ |
| InsertARowWithNullableFloatValueAndNullString | to test the construction of the block that contains the column with definition of `nullable (Float)`  and the other column with definition of `nullable (String) `| ++ | 
| InsertMultipleRowsWithNullableFloatValueAndNullString | to test the construction of the block that contains the column with definition of `nullable (Float)`  and the other column with definition of `nullable (String) `, and have the block to contain multiple rows in a batch| ++ | 
| InsertSingleRowWithNullStringForQATestingTableAndOrderMatched | to test the construction of the block that contains the column with definition of `nullable (String) `, with the full schema defined as the same in the QA staging environment| ++ | 
| InsertSingleRowWithNullStringForQATestingTableAndOrderMatched | to test the construction of the block that contains the column with definition of `nullable (String) `, with the full schema defined as the same in the QA staging environment and the column ordering is the same as the schema defined| ++ | 
| InsertSingleRowWithNullStringForQATestingTableAndOrderNotMatched | to test the construction of the block that contains the column with definition of `nullable (String) `, with the full schema defined as the same in the QA staging environment and the column ordering is different from what the schema has defined, thus column shuffle is needed| ++ | 

## `test_protobuf_reader.cpp`

It tests the class of `ProtobufBatchReader`.


| Test function | Explanation | Need Stronger Assertion | 
|---------------|-------------| ------------------------|
| testExtractColumnNames1 | to test the extraction of the column names from the insert statement passed from the Service. In this case, no specific columns are named in the insert statement. | ++ | 
| testExtractColumnNames2 | to test the extraction of the column names from the insert statement passed from the Service. In this case, some specific columns are named in the insert statement.| ++ | 
| testExtractColumnNamesWithSingleQuotes | to test the extraction of the column names from the insert statement passed from the Service. In this case, some specific columns are named in the single quote string.|  ++ | 
| testExtractColumnNamesdWithoutSingleQuotes | to test the extraction of the column names from the insert statement passed from the Service. In this case, some specific columns are named without the single quotes.| ++ | 
| testRetrieveTableDefinitionWithoutDefaults1 | to test the extraction of the column names from the insert statement passed from the Service. No default columns are involved in the specified columns and the full table definition does not involve default columns at all.|  ++ | 
|testRetrieveTableDefinitionWithoutDefaults2 | to test the extraction of the column names from the insert statement passed from the Service. No default columns are involved in the specified columns. But some missing  (default) columns do come from  the table definition| ++ |
|testRetrieveTableDefinitionWithDefaults1 | to test the extraction of the column names from the insert statement passed from the Service. All columns are specified in the insert statement, following the full table definition. No default columns are missing from the insert statement| ++ | 
|testRetrieveTableDefinitionWithDefaults2 | to test the extraction of the column names from the insert statement passed from the Service. Some columns are specified in the insert statement, and some of them are with default expression.| ++ | 


## `test_serializer_loader.cpp`

It tests the class of `ProtobufBatchReader` and `AggregatorLoader`.


| Test function | Explanation | Need Stronger Assertion| 
|---------------|-------------| -----------------------|
| InsertARowToCHServerWithBlockConstructedFromMessageWithSingleRow | to test the construction of the block from a single row based on the protobuf message, perform  message de-serialization, and push the block to the local ClickHouse server | ++ | 
| InsertARowToCHServerWithBlockConstructedFromMessageWithMultipleRows | to test the construction of the block from multile rows embedded on the protobuf message, perform message de-serialization, and push the block to the local ClickHouse server | ++ | 


## `test_tabledefinition_retrievals.cpp`

It tests the class of `AggregatorLoader`, with different kinds of the query embedded, and the returns are in blocks.


| Test function | Explanation | Need Stronger Assertion |
|---------------|-------------| ----------------------- |
| testRetrieveAllTables | to test the retrieval of all of the table definitions held in the "default" local database| ++ | 


