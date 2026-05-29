# Connector Configuration Properties
The following is a list of options and settings that can be configured for the source and sink connectors. 
<br>

## Source Configuration Properties
### Required
* **"ampsTopic":** Topic or regular expression topic for an AMPS subscription, the topic Source will be subscribed to.

<br>

* **"clientName":** The name of the AMPS client.

<br>

* **"uri":** AMPS server URI(s):  

    * scheme: `"//[userinfo@]host[:port]/"`.
    
    * a comma separated list can be provided for use with multiple URIs.

<br>

### Optional
* **"bookmark":** The bookmark for the subscription. Accepts any valid AMPS bookmark.

    * **"bookmarkStoreType":** The bookmark store type, logged or memory.

    * **"bookmarkLog":** The bookmark log file path/name if using a logged bookmark store.

    * **"pruneTimeThreshold":** The pruning time threshold for a bookmark log.

<br>

* **"clientFactoryClass":** The name of the class to be used to construct the AMPS client from configuration.

    * default provided class: `"com.crankuptheamps.kafka.AMPSBasicClientFunction"`.

<br>

* **"cmdTypeFilter":** A bit mask of the AMPS command type integers. 

<br>

* **"command":** AMPS command type: subscribe or sow_and_subscribe. Defaults to subscribe.

<br>

* **"eventHeaders":** A comma separated list of headers we want to preserve.

    - **commandHeader -** The command type of the AMPS message.

    - **topicHeader -** The topic name of the AMPS message.

    - **sowKey -** The SOW key of the AMPS message. Only set on the message for SOW and SOW and Subscribe queries.

    - **ampsTimestamp -** The ISO-8601 timestamp of when AMPS processed the message.
    
    - **bookmarkHeader -** The unique bookmark string the AMPS message. Only set on the message for bookmark subscriptions.
    
    - **correlationId -** The correlation ID of the AMPS message.
    
    - **subId -** The subscription ID of the AMPS message.
    
    - **length -** The length of the AMPS message body in bytes.
    
    - **timestamp -** The current timestamp of when the AMPS Kafka source received the message.

<br>

* **"filter":** The filter expression.

<br>

* **"isQueue":** The indicator of whether the topic being subscribed to is a queue: `"y"` if using a queue.

    * **"ackBatchSize":** The maximum size of acknowledgment batches.

    * **"ackTimeout":** The maximum amount of time to wait before acknowledging a batch of messages.

    * **"maxBacklog":** The maximum number of messages that have been provided to a queue subscription that have not yet been acknowledged.

<br>

* **"key":** The Kafka record key to publish to the same partition and maintain ordering.

<br>

* **"maxBatch":** The max number of records that should be returned to Kafka in each batch.

<br>

* **"maxBuffers":** The max number of message buffers.

<br>

* **"options":** The AMPS command options.

<br>

* **"subscriptionId":** The AMPS subscription ID. Required when using a bookmark subscription.

<br><br>

## Sink Configuration Properties
### Required
* **"ampsTopic":** An AMPS topic that we publish to. Overriden when `"useTopicHeader"` is true.

<br>

* **"clientName":** The name of the AMPS client.

<br>

* **"uri":** AMPS server URI(s): 

    * scheme: `"//[userinfo@]host[:port]/"`.

    * a comma separated list can be provided for use with multiple URIs.

<br>

### Optional
* **"clientFactoryClass":** The name of the class to be used to construct the AMPS client from configuration.

    * default provided class: `"com.crankuptheamps.kafka.AMPSBasicClientFunction"`.

<br>

* **"logMsgOnWriteError":**

    - `"true"`: Write the log message on write error.

    - `"false"`: Do not write the log message on write error.

<br>

* **"maxBatch":** The max number of records that should be returned to be published to AMPS.

<br>

* **"pubStoreType":** The publish store type: memory or file.

    * **"pubStoreInitialCap":** The publish store's initial capacity.

    * **"pubStorePath":** The publish store's file path if using a file-backed publish store.

    * **"pruneTimeThreshold":** The pruning time threshold for a file backed store.

<br>

* **"useTopicHeader":**

    - `"true"`: Publish to the topic specified in the message's topic header.

    - `"false"`: Publish to the specified topic in the configuration.

<br><br>

## Kafka Connect Configuration Properties
* **"connector.class":** The implementing class determining the type of connector.

    - `"com.crankuptheamps.kafka.AMPSKafkaSink"`.

    - `"com.crankuptheamps.kafka.AMPSKafkaSource"`.

<br>

* **"tasks.max":** Specifies how many instances of the connector should run in parallel. Recommended: 1

<br>

* **"topics":** The topic(s) that the sink connector will collect records from. Not in source connector.

<br>

* **"topics.regex":** A Java regular expression of topics that the sink connector will collect records from. Not in source connector.

<br>

* Defined converter classes that serialize and deserialize the data. Can be configured in Connect Worker or in configuration properties:

    - **"key.converter":** `"org.apache.kafka.connect.storage.StringConverter"`.

    - **"value.converter":** `"org.apache.kafka.connect.converters.ByteArrayConverter"`.

    - **"header.converter":** `"org.apache.kafka.connect.storage.StringConverter"`.
