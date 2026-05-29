# AMPS Kafka Connector
This project enables seamless integration between AMPS and Apache Kafka. It provides a custom pollable Kafka Source to stream data from AMPS into Kafka, and a Kafka Sink to publish data from Kafka into AMPS.
<br>

### Prerequisites
1. **Java**
    * Java 8 or higher
    * **Note:** If using Kafka 4.0+, you must have Java 17 or higher

2. **Maven** 
    * Maven 3.3+

3. **[AMPS](https://www.crankuptheamps.com/)** 
    * AMPS Server

    * AMPS Java Client

4. **[Apache Kafka](https://kafka.apache.org)**
    * Kafka 2.13 or higher

<br><br>

## Installation
1. **Build the Connector:** Navigate to the project directory and run:
    ```
    mvn clean package -DskipTests
    ```
    The JAR will be generated at:
    ```
    target/amps-kafka-connector-1.2.0.jar
    ```

2. **Plugin Setup:** To load the connector into your Kafka Connect environment:

    * Create a `plugins` directory (if not already present).

    * Create the following structure and add the required JAR files. 
        ```
        plugins/
            amps/
                amps-kafka-connector-1.2.0.jar
                amps_client.jar
        ```
    
3. **Kafka Connect Configuration:**
   * **For Standalone Mode:** Update `$kafka_{version}/config/connect-standalone.properties`:
    ```
    plugin.path=/path/to/your/plugins
    ```

    * **For Distributed Mode:** Update `$kafka_{version}/config/connect-distributed.properties`:
    ```
    plugin.path=/path/to/your/plugins
    ```
<br><br>

## Quickstart
Included in the project repository is a working example highlighting the powerful aggregated subscription feature of AMPS.

The example will use a single-node Kafka cluster, acting as both the source and the sink. Messages are first published to AMPS, then streamed to Kafka using the source connector. Then, these messages are routed back into AMPS using the sink connector.

1. **Start the Kafka Server:**

    * Navigate to the `kafka_{version}` directory.

    * **Using KRaft:**
        * Generate the Cluster UUID
            ```
            KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
            ```
        * Format Log Directories
            ```
            bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
            ```
        * Start the Kafka Server
            ```
            bin/kafka-server-start.sh config/server.properties
            ```

    * **Using ZooKeeper (legacy):**
        * Start the ZooKeeper Server
            ```
            bin/zookeeper-server-start.sh config/zookeeper.properties
            ```
        * Start the Kafka Server
            ```
            bin/kafka-server-start.sh config/server.properties
            ```

2. **Start Kafka Connect:**

    * **Distributed Mode** (recommended):
        * Start the distributed worker:
            ```
            bin/connect-distributed.sh config/connect-distributed.properties
            ```

        * Load connectors:
            ```
            curl -X POST -H "Content-Type:application/json" http://localhost:8083/connectors --data @amps-integration-apache-kafka/src/test/resources/examples/aggSub/AMPSKafkaSource.json
            curl -X POST -H "Content-Type:application/json" http://localhost:8083/connectors --data @amps-integration-apache-kafka/src/test/resources/examples/aggSub/AMPSKafkaSink.json
            ```

    * **Standalone Mode** (also supported):
        * Start the worker and load the connectors
            ```
            bin/connect-standalone.sh config/connect-standalone.properties \
                amps-integration-apache-kafka/src/test/resources/examples/aggSub/connect-amps-kafka-source.properties \
                amps-integration-apache-kafka/src/test/resources/examples/aggSub/connect-amps-kafka-sink.properties
            ```

3. **Start AMPS:** Use the configuration file located at `amps-integration-apache-kafka/src/test/resources/examples/aggSub/amps-config.xml`.
    ```
    AMPS-{version}-Release-Linux/bin/ampServer amps-integration-apache-kafka/src/test/resources/examples/aggSub/amps-config.xml
    ```

4. **Subscribe** to the `AMPSKafkaSinkTest` topic on the AMPS server using the AMPS spark utility:
    ```
    AMPS-{version}-Release-Linux/bin/spark subscribe -server localhost:9007 -topic AMPSKafkaSinkTest
    ```

5. **Publish** the example JSON messages to the `Orders` topic on the AMPS server using the AMPS spark utility:
    ```
    AMPS-{version}-Release-Linux/bin/spark publish -server localhost:9007 -topic Orders -rate 1 -file amps-integration-apache-kafka/src/test/resources/examples/aggSub/messages.json
    ```

6. Notice that we are publishing at a rate of 1 message per second, allowing us to observe the aggregate fields change over time within a conflation interval of 1 second. Although 15 messages are published, the AMPS subscription will receive 10 updates due to the following filter in our source connector configuration file, `/symbol IN ('IBM', 'MSFT')`.
<br>
<br>

Please send any issues or questions to support@crankuptheamps.com
