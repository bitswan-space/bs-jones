# BS-Jones
BS-Jones tackles the archaisms of the SAP Sybase database, executing a query, transforming it to events and loading those to Kafka.


The pump loads sql_query file as a sql query and has ability to format the query with the current time. Specify the time that will be changed by {current_time} in the sql query.

## Example configuration

```
#daily parameter must contain integer 1 -> True or 0 -> false
[sybase]
server=jerusalem
database=ark_of_the_covenant
username=Jones
password=staff_of_ra
driver={U-Boat}
host=localhost
port=3456
query=/conf/sql_query
query_interval=* * * * *
resolution=24*60
daily=true


[connection:KafkaConnection]
bootstrap_servers=kafka-1:9092,kafka-2:9092,kafka-3:9092

[pipeline:BSJonesPipeline:KafkaSink]
topic=testP-eniq



```

## Example docker-compose
```
version: '3.8'
services:
  bs-jones:
    image: bs-jones
    network_mode: host
    env_file:
      - ./site/.env
    volumes:
      - ./site/odbcinst.ini:/etc/odbcinst.ini
      - ./site/site.conf:/conf/bs-jones.conf
      - ./site/sql_query:/conf/sql_query
```
