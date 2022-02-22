# BS-Jones
BS-Jones tackles the archaisms of the SAP Sybase database, executing a query, transforming it to events and loading those to Kafka.

## Example configuration

```
[sybase]
server=jerusalem
database=ark_of_the_covenant
username=Jones
password=staff_of_ra
driver={U-Boat}
host=localhost
port=3456

[connection:KafkaConnection]
bootstrap_servers=kafka-1:9092,kafka-2:9092,kafka-3:9092

[pipeline:BSJonesPipeline:KafkaSink]
topic=testP-eniq

```
