# debezium-hbase-sink
A kafka connect sink specifically from (source) debezium-postgres or debezium-mysql to (sink) HBase.

Debezium is a great project for Postgres/MySQL CDC atop of the logical decoding feature(https://github.com/debezium/debezium).

## [Debezium-postgres](http://debezium.io/docs/connectors/postgresql/)
PostgreSQL’s logical decoding feature was first introduced in version 9.4 and is a mechanism which allows the extraction of the changes which were commited to the transaction log and the processing of these changes in a user-friendly manner via the help of an output plugin.

## [Debezium-mysql](http://debezium.io/docs/connectors/mysql/)

## debezium/decoderbufs
Any changes commit to PG tables are captured and the change events are sent to kafka through kafka connect source task. The events are Protobuf formated (https://github.com/debezium/postgres-decoderbufs).

We want to poll the change events out from kafka,'decode' them and put to HBase.

## Goal
So, rather than a generic hbase-sink for kafka connect (such as https://github.com/mravi/kafka-connect-hbase), this project is very specific:
- it is a hbase sink for kafka connect,
- BUT it consumes only Debezium-Postgres CDC generated by decoderbufs (github.com/debezium/postgres-decoderbufs)

## Build
1. clone the git repository  
2. `mvn clean package` or `mvn clean package -DskipTests`

## Configurations

| Item               	| Importance 	| Default 	| Comment                                    	|
|--------------------	|------------	|---------	|--------------------------------------------	|
| name              | High          |          | Unique name of the Connector |
| connector.class | High    | com.hon.saas.hbase.sink.HBaseSinkConnector | Sink connector to HBase|
| tasks.max             | High          |  1        | Has to be in 1 to make sure CDC happenes in sequence|
| zookeeper.quorum   	| High       	|         	| Zookeeper quorum of HBase cluster          	|
| event.parser.class 	| High       	|         	| Event parser class to parse the SinkRecord   |
| topics                  	| High           	|         	| Topics to be comsumed from, comma separated. Topic name will be mapped to the name of HBase table     	|
| hbase.%s.rowkey.columns  | High  |     | rowkey columns per kafka topic. Each topic needs to define its own rowkey.columns  |
| hbase.%s.rowkey.family  | Medium |  F   | Column family for corresponding HBase table |

### Event Class  
The event calss can be one of the followings:  
1. com.hon.saas.hbase.parser.DebeziumCDCAvroEventParser  
`Note: targeted for the Debezium CDC messages`  
2. com.hon.saas.hbase.parser.AvroEventParser  
`Note: common Avro message`    
3. com.hon.saas.hbase.parser.JsonEventParser  
`Note: common JSON message`

## Examples  
1. [Debezium Connector(MYSQL -> Kafka)](./examples/debezium_mysql_to_kafka.properties "Debezium Connector(MYSQL -> Kafka)")
2. [Debezium HBase Sink Connector(Kafka -> HBase)](./examples/debezium_kafka_to_hbase.properties "HBase Sink Connector(Kafka -> HBase)")

