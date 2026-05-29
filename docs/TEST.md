# AMPS Kafka Connect Tests

The JUnit tests under `src/test/java` automate the connector example flows. They use AMPS clients, Kafka, and the Kafka Connect REST API. Kafka, Kafka Connect, and two AMPS servers must already be running before `mvn test` starts.


## Installation

1. Ensure you have all the [prerequisites](../README.md#prerequisites) installed.

2. Install the AMPS Kafka Connect plugin using the provided [installation instructions](../README.md#installation).
These tests assume Kafka Connect is running in distributed mode.
<br><br>

## Setup

1. Start Kafka.

    For a KRaft Kafka installation, format storage once if needed and start the broker:

    ```
    cd kafka_{version}
    KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
    bin/kafka-server-start.sh config/server.properties
    ```

    For a ZooKeeper-based Kafka installation, start ZooKeeper and then Kafka:

    ```
    cd kafka_{version}
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
    ```

2. Start Kafka Connect in distributed mode.

    ```
    cd $KAFKA_HOME
    export KAFKA_HEAP_OPTS="-Xms2G -Xmx2G"
    bin/connect-distributed.sh config/connect-distributed.properties
    ```

    The tests create and delete the required connectors through the Kafka Connect REST API. Do not create the example connectors manually.

3. Start both AMPS servers in separate terminals from the `amps-integration-apache-kafka` project directory.

    ```
    AMPS-{version}-Release-Linux/bin/ampServer src/test/resources/amps-configs/testConfig.xml
    ```

    ```
    AMPS-{version}-Release-Linux/bin/ampServer src/test/resources/amps-configs/testConfigReplica.xml
    ```

    The primary server listens on `tcp://localhost:9007/amps/json`. It also exposes a dedicated failover transport at `tcp://localhost:9011/amps/json` that tests can disable and re-enable through AMPS actions. The replica server listens on `tcp://localhost:9009/amps/json`.

## Run

Run the full integration suite from the `amps-integration-apache-kafka` project directory:

```
mvn test
```

Default endpoints:

```
amps.uri=tcp://localhost:9007/amps/json
amps.failover.primary.uri=tcp://localhost:9011/amps/json
amps.failover.secondary.uri=tcp://localhost:9009/amps/json
kafka.bootstrap.servers=localhost:9092
kafka.connect.url=http://localhost:8083
```

Override endpoints as Maven properties when your environment uses different ports:

```
mvn test \
  -Damps.uri=tcp://localhost:9007/amps/json \
  -Damps.failover.primary.uri=tcp://localhost:9011/amps/json \
  -Damps.failover.secondary.uri=tcp://localhost:9009/amps/json \
  -Dkafka.bootstrap.servers=localhost:9092 \
  -Dkafka.connect.url=http://localhost:8083
```

To run a single test:

```
mvn test -Dtest=AMPSKafkaExampleIntegrationTest#failoverTest
```

## Included Tests

`basicFlowTest` verifies the basic AMPS source to Kafka to AMPS sink path with fixed-size JSON messages.

`dynamicSubTest` verifies regex topic subscription and sink publication using Kafka topic headers.

`deliveryQueueTest` verifies AMPS queue consumption through Kafka and delivery back to an AMPS sink topic.

`bookmarkTest` verifies the memory and logged bookmark source configurations.

`aggregateTest` verifies aggregate subscription filtering, grouping, projection, and sink delivery.

`failoverTest` verifies that HA source and sink connectors continue delivering messages after the dedicated primary failover AMPS TCP transport is disabled.
