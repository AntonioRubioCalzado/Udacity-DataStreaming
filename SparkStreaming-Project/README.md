# Spark Streaming Project
## Execution of modules
- Once the files **config/zookeeper.properties** and **config/server.properties** have been defined it's necessary to type the following commands in shell and so start both zookeeper as kafka server:

`>> /usr/bin/zookeeper-server-start zookeeper.properties`
`>> /usr/bin/kafka-server-start server.properties`

- Run the **producer_server.py** and **kafka_server.py** to send the entries of the file *police-department-calls-for-service.json* into a Kafka topic. In CLI, type:

`>> python producer_server.py`
`>> python kafka_server.py`

- Check that data is sent properly into that topic by typing into CLI the command `kafka-console-consumer --topic <TOPIC_NAME> --bootstrap-server <PORT> --from-beginning` or , alternatively, by running the consumer script:

`>> python kafka_consumer.py`

- Execute the Spark Streaming Script that reads and processes the data in the previous topic by writing the following command in the shell:

`>> spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py` 


## Questions
### Question 1
*How did changing values on the SparkSession property parameters affect the throughput and latency of the data?*

They change the parameters `inputRowsPerSecond` and `processedRowsPerSecond`.
    
### Question 2
*What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?*

To study variations of `inputRowsPerSecond` and `processedRowsPerSecond` we've made hypertunning over the parameters `spark.sql.shuffle.partitions`,  `spark.streaming.kafka.maxRatePerPartition` and `spark.default.parallelism`.

Let's share some examples tested:

```
spark.sql.shuffle.partitions = 25
spark.streaming.kafka.maxRatePerPartition = 25
spark.default.parallelism = 2500

"inputRowsPerSecond" : 13.265782281181256,
"processedRowsPerSecond" : 85.66508824795523
```

```
spark.sql.shuffle.partitions = 12
spark.streaming.kafka.maxRatePerPartition = 12
spark.default.parallelism = 12000
 
"inputRowsPerSecond" : 12.225213100044863,
"processedRowsPerSecond" : 47.74419623302672
```

```
spark.sql.shuffle.partitions = 12
spark.streaming.kafka.maxRatePerPartition = 12
spark.default.parallelism = 1200

"inputRowsPerSecond" : 14.223922808184994,
"processedRowsPerSecond" : 70.80745341614907
```

But the optimal result tested has been the next one:

```
spark.sql.shuffle.partitions = 11
spark.streaming.kafka.maxRatePerPartition = 11
spark.default.parallelism = 11000

"inputRowsPerSecond" : 14.9,
"processedRowsPerSecond" : 113.740458015267178
```