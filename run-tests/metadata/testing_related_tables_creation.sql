CREATE TABLE default.aliases_lazyness (`x` UInt32, `y` UInt8 ALIAS sleepEachRow(0.1)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192;

CREATE TABLE default.aliases_lazyness3 (`x` UInt32, `y` Int64 ALIAS runningDifference(x)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192;

CREATE TABLE default.data_validation (`RoundId` Int64, `RowId` Int64, `AggregatorId` String, `AggregatorTime` DateTime) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/data_validation', '{replica}') PARTITION BY intDiv(toUnixTimestamp(AggregatorTime), 86400) 
  ORDER BY (RoundId, RowId) SETTINGS index_granularity = 8192;


CREATE TABLE default.local_test (`trace_id` String, `log_id` String, `t` UInt64, `db_statement` String, 
  `db_statement_id` String MATERIALIZED lower(hex(sipHash128(db_statement))), `date` Date)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_test', '{replica}') 
  PARTITION BY toRelativeDayNum(date)
  ORDER BY t SETTINGS index_granularity = 8192;

CREATE TABLE default.my_table (`date` Date DEFAULT today(), `s` String) ENGINE = MergeTree(date, date, 8192);

CREATE TABLE default.ontime (`year` UInt16, `quarter` UInt8, `month` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, `flightDate` Date)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (year, flightDate) 
  ORDER BY (year, flightDate) SETTINGS index_granularity = 8192;

CREATE TABLE default.ontime_2 (`year` UInt16, `quarter` UInt8, `month` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, `flightDate` Date) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime_2', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (year, flightDate) 
  ORDER BY (year, flightDate) SETTINGS index_granularity = 8192;


CREATE TABLE default.ontime_for_schema_matching (`flightYear` UInt16, `quarter` UInt8 DEFAULT CAST('1', 'UInt8'),
  `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, `flightDate` Date) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime_for_schema_matching', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192;


CREATE TABLE default.ontime_with_nullable_extended (
   `flightYear` UInt16, `quarter` UInt8, `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8,
   `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), 
   `status` String DEFAULT 'normal',
   `status_added_1` UInt32 DEFAULT 2,
   `status_added_2` String DEFAULT 'aaa'
   ) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime_with_nullable_extended', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192;

CREATE TABLE default.ontime_with_nullable (`flightYear` UInt16, `quarter` UInt8, `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8,
   `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), `status` String DEFAULT 'normal') 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime_with_nullable', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192;


CREATE TABLE default.ontime_with_nullable_2 (`flightYear` UInt16, `quarter` UInt8, `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8,
  `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), `status` String DEFAULT 'normal') 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime_with_nullable_2', '{replica}')
  PARTITION BY flightDate 
  PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event (`Host` String, `Colo` String, `EventName` String, `Count` UInt64, `Duration` Float32, `TimeStamp` DateTime) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event', '{replica}') 
  PARTITION BY intDiv(toUnixTimestamp(TimeStamp), 86400) 
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_14 (`Counter` UInt64, `Host` FixedString(12), `Colo` FixedString(12))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_14', '{replica}')
  ORDER BY (Host, Counter) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_16 (`Counter` UInt64, `Host` FixedString(12), `Colo` FixedString(12), `FlightDate` Date) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_16', '{replica}')
  ORDER BY (Host, Counter) SETTINGS index_granularity = 8192;

CREATE TABLE default.simple_event_18 (`Counter` UInt64, `Host` LowCardinality(String), `Colo` LowCardinality(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_18', '{replica}')
  ORDER BY (Host, Counter) SETTINGS index_granularity = 8192;

CREATE TABLE default.simple_event_20 (`Counter` UInt64, `Host` LowCardinality(String), `Colo` LowCardinality(String), `FlightDate` Date)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_20', '{replica}')
  ORDER BY (Host, Counter) SETTINGS index_granularity = 8192;

CREATE TABLE default.simple_event_22 (`Counter` Nullable(UInt64), `Host` String, `Colo` String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_22', '{replica}')
  ORDER BY Host SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_24 (`Counter` UInt64, `Host` Nullable(String), `Colo` Nullable(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_24', '{replica}')
  ORDER BY Counter SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_26 (`Counter` UInt64, `Host` Nullable(String), `Colo` Nullable(String), `FlightDate` Date)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_26', '{replica}')
  ORDER BY Counter SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_3 (`Host` String, `Count` UInt64)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_3', '{replica}')
  ORDER BY (Host, Count) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_34 (`Counter` UInt64, `Host` String, `Colo` String, `FlightDate` Date)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_34', '{replica}')
  ORDER BY (Host, Counter) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_5 (`Count` UInt64, `Host` String, `Colo` String) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_5', '{replica}')
  ORDER BY (Host, Count) SETTINGS index_granularity = 8192;

CREATE TABLE default.simple_event_7 (`Count` UInt64, `Host` String, `Colo` String) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_7', '{replica}')
  ORDER BY (Host, Count) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_event_9 (`Time` Date DEFAULT today(), `Host` String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_9', '{replica}')
  ORDER BY (Host, Time) SETTINGS index_granularity = 8192;

CREATE TABLE default.simple_lowcardinality_event_3 (`Host` String, `Colo` LowCardinality(FixedString(8)), `EventName` LowCardinality(String),
  `Count` UInt64, `Duration` Nullable(Float32), `Description` Nullable(String)) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_lowcardinality_event_3', '{replica}')
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_lowcardinality_event_4 (`Host` String, `Colo` LowCardinality(FixedString(8)), `EventName` LowCardinality(String),
  `Count` UInt8, `Duration` Nullable(Float32), `Description` Nullable(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_lowcardinality_event_4', '{replica}')
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_lowcardinality_event_5 (`Host` String, `Colo` LowCardinality(FixedString(8)), `EventName` LowCardinality(FixedString(8)),
  `Count` UInt64, `Duration` Nullable(Float32), `Description` Nullable(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_lowcardinality_event_5', '{replica}')
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_nullable_event (`Host` String, `Colo` FixedString(8), `EventName` String, 
  `Count` UInt64, `Duration` Nullable(Float32), `Description` Nullable(String), `TimeStamp` DateTime)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_nullable_event', '{replica}')
  PARTITION BY intDiv(toUnixTimestamp(TimeStamp), 86400)
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.simple_nullable_event_2 (`Host` String, `Colo` FixedString(8), `EventName` String, 
  `Count` UInt64, `Duration` Nullable(Float32), `Description` Nullable(String)) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_nullable_event_2', '{replica}')
  ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192;


CREATE TABLE default.t_null (`x` Int8, `y` Nullable(Int8)) ENGINE = TinyLog;

CREATE TABLE default.typetable_with_default (`Id` Int64, `SmallInt` Int8, `SmallUInt` UInt8, 
  `MediumInt` Int32, `MediumUInt` UInt32, `LargeInt` Int64, `LargeUInt` UInt64, `SomeFloat` Float64,
  `SomeString` String, `SomeFixedString25` FixedString(25), `SomeDate` Date, `SomeStringWithDefExpr` String DEFAULT 'normal')
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/typetable_with_default', '{replica}')
  PARTITION BY SomeDate PRIMARY KEY Id
  ORDER BY Id SETTINGS index_granularity = 8192;

CREATE TABLE default.xdr_tst (`date` Date, `answered` Int8 DEFAULT -1, `end_date` UInt64, `start_date` UInt64)
  ENGINE = MergeTree(date, (start_date, end_date), 8192);

CREATE TABLE default.xdr_tst2 (`date` Date DEFAULT today(), `answered` Int8 DEFAULT -1, `end_date` UInt64, `start_date` UInt64)
  ENGINE = MergeTree(date, (start_date, end_date), 8192);


CREATE TABLE default.xdr_tst5 (`date` Date DEFAULT today(), `answered` Int8 DEFAULT CAST(0, 'Int8'), `end_date` UInt64, `start_date` UInt64)
  ENGINE = MergeTree(date, (start_date, end_date), 8192);


CREATE TABLE simulated_simple_ads_pl0 (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, `itemId` Int64,
  `selleerKwId` String DEFAULT 'normal', `adgroupId` Nullable(String), `compaignType` LowCardinality(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simulated_simple_ads_pl0', '{replica}') 
  PARTITION BY toYYYYMMDD(eventts)
  ORDER BY (sellerId, compaignId, compaignType) SETTINGS index_granularity = 8192 ;

CREATE TABLE simulated_simple_ads_pl0_missing_columns (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, `itemId` Int64)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simulated_simple_ads_pl0_missing_columns', '{replica}') 
  PARTITION BY toYYYYMMDD(eventts) 
  ORDER BY (sellerId, compaignId) SETTINGS index_granularity = 8192 ;


CREATE TABLE simulated_simple_ads_pl1 (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, 
  `itemId` Int64, `selleerKwId` String DEFAULT 'normal', `adgroupId` Nullable(String), `compaignType` LowCardinality(String))
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simulated_simple_ads_pl1', '{replica}') 
  PARTITION BY toYYYYMMDD(eventts)
  ORDER BY (sellerId, compaignId, compaignType) SETTINGS index_granularity = 8192 ;


CREATE TABLE simulated_simple_ads_pl1_missing_columns (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, `itemId` Int64)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simulated_simple_ads_pl1_missing_columns', '{replica}') 
  PARTITION BY toYYYYMMDD(eventts) 
  ORDER BY (sellerId, compaignId) SETTINGS index_granularity = 8192 ;


CREATE TABLE default.simple_event_56 (`Counter` UInt64, `ShortTime` DateTime, `LongTime` DateTime64, 
  `LongTimeNullable` Nullable(DateTime64(3, 'America/Phoenix'))) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_56', '{replica}') 
  ORDER BY Counter SETTINGS index_granularity = 8192 ;

create table simple_event_62 (
     Counters Array(UInt32),
     Host String,
     Colo String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_62', '{replica}') 
  ORDER BY(Host) SETTINGS index_granularity=8192;

create table simple_event_63 (
     Counters Array(Array(UInt32)),
     Host String,
     Colo String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_63', '{replica}') 
  ORDER BY(Host) SETTINGS index_granularity=8192;


create table simple_event_64 (
     Counters Array(Array(Nullable(String))),
     Host String,
     Colo String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_64', '{replica}') 
  ORDER BY(Host) SETTINGS index_granularity=8192;

create table simple_event_65 (
     Counters Array(Array(Nullable(String))) default [['aaa', NULL]],
     Host String,
     Colo String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_65', '{replica}') 
  ORDER BY(Host) SETTINGS index_granularity=8192;


create table simple_event_66 (
     Counters1 Array(Array(UInt32)),
     Counters2 Array(Array(Nullable(String))) default [['aaa', NULL]],
     Host String,
     Colo String)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_66', '{replica}') 
  ORDER BY(Host) SETTINGS index_granularity=8192;


CREATE TABLE default.simple_event_68 (`Count` UInt64, `Host` String, `Colo` String, `Counters` Array(Array(UInt32))) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_68', '{replica}') 
  ORDER BY (Host, Count) SETTINGS index_granularity = 8192;


CREATE TABLE default.block_default_missing_ontime_test_0 (`flightYear` UInt16, `quarter` UInt8, 
  `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, 
  `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), `status` String DEFAULT 'normal') 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/block_default_missing_ontime_test_0', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192 ;


CREATE TABLE default.block_default_missing_ontime_test_1 (`flightYear` UInt16, `quarter` UInt8, 
  `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, 
  `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), 
  `status` String DEFAULT 'normal', `column_new_1` UInt64 DEFAULT CAST('0', 'UInt64')) 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/block_default_missing_ontime_test_1', '{replica}') 
  PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192 ;



CREATE TABLE default.block_default_missing_ontime_test_2 (`flightYear` UInt16, `quarter` UInt8, 
  `flightMonth` UInt8, `dayOfMonth` UInt8, `dayOfWeek` UInt8, 
  `flightDate` Date, `captain` Nullable(String), `code` FixedString(4), 
  `status` String DEFAULT 'normal', `column_new_1` UInt64 DEFAULT CAST('0', 'UInt64'), `column_new_2` String DEFAULT '') 
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/block_default_missing_ontime_test_2', '{replica}') 
  PARTITION BY flightDate 
  PRIMARY KEY (flightYear, flightDate) 
  ORDER BY (flightYear, flightDate) SETTINGS index_granularity = 8192 ;
