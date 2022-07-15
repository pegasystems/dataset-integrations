## Simple kafka load generator
Simple kafka client which generates random string messages and sends it to Kafka broker.
It can be used for performance testing, though it's functionality is very limited.

## How to use

1. Build the project
2. Create a property file `msk.properities` with following content:
```
threads=1
throughput=100
message.size=10000
message.count=100000000
topic.name=__TOPIC_NAME__

producer.batch.size=65536
producer.delivery.timeout.ms=120000
producer.linger.ms=5
producer.max.block.ms=10000
producer.max.request.size=5000000
producer.retries=1
producer.acks=1

producer.bootstrap.servers=__HOST:PORT__
producer.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
producer.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-512
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
username\="alice" \
password\="alice-secret";

```
3. Run: `java -jar kafka-test-1.0-SNAPSHOT.jar msk.properties`

### Examples

#### Running time limited load using 1 thread

```timeout 1m java -Dlog4j.configuration=file:"./log4j.trace" -jar kafka-test-1.0-SNAPSHOT.jar 1th-100rs.properties > 1th-100rs.log```

If number of generated messages is too big, we can limit total time of execture of `java ...` command,
by using `timeout 1m`. 
To have more in depth look at Kafka client operations we will override log4j configuration with `log4j.trace`. Default log level is DEBUG
`1th-100rs.properties` configuration file is similar to the example above, but number of threads is 1.
`>` will redirect output to the file  `1th-100rs.log` for later analysis.

