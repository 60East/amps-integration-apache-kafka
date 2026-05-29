///////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2024-2026 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

package com.crankuptheamps.kafka;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MemoryBookmarkStore;
import com.crankuptheamps.client.MemoryPublishStore;
import com.crankuptheamps.kafka.testutils.AMPSMessageHandler;
import com.crankuptheamps.kafka.testutils.AMPSMessageHandler.ReceivedMessage;
import com.crankuptheamps.kafka.testutils.ExampleTestUtils;
import com.crankuptheamps.kafka.testutils.KafkaConnectClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AMPSKafkaExampleIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(AMPSKafkaExampleIntegrationTest.class);

    private static final String AMPS_URI =
            System.getProperty("amps.uri", "tcp://localhost:9007/amps/json");
    private static final String AMPS_FAILOVER_PRIMARY_URI =
            System.getProperty("amps.failover.primary.uri", "tcp://localhost:9011/amps/json");
    private static final String AMPS_FAILOVER_SECONDARY_URI =
            System.getProperty("amps.failover.secondary.uri", "tcp://localhost:9009/amps/json");
    private static final String KAFKA_CONNECT_URL =
            System.getProperty("kafka.connect.url", "http://localhost:8083");
    private static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String QUEUE_TOPIC = "QueueSourceTest";
    private static final String ENABLE_FAILOVER_TRANSPORT_TOPIC = "enableKafkaFailoverTransport";
    private static final String DISABLE_FAILOVER_TRANSPORT_TOPIC = "disableKafkaFailoverTransport";

    private static final long CONNECTOR_START_TIMEOUT_MS = 30000L;
    private static final long MESSAGE_TIMEOUT_MS = 60000L;

    private KafkaConnectClient kafkaConnect;
    private final List<String> connectorNames = new ArrayList<>();

    @BeforeClass
    public static void requireRunningServices() throws Exception {
        LOG.info("Checking services: AMPS={}, Kafka={}, Kafka Connect={}",
                AMPS_URI, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONNECT_URL);
        requireAmps(AMPS_URI);
        new KafkaConnectClient(KAFKA_CONNECT_URL).assertRunning();
        requireKafka();
    }

    @Before
    public void createClient() {
        kafkaConnect = new KafkaConnectClient(KAFKA_CONNECT_URL);
    }

    @After
    public void deleteCreatedConnectors() throws Exception {
        for (String connectorName : connectorNames) {
            kafkaConnect.deleteConnector(connectorName);
        }
        connectorNames.clear();
    }

    // Verifies the basic source-to-Kafka-to-sink path with fixed-size JSON messages.
    @Test(timeout = 90000L)
    public void basicFlowTest() throws Exception {
        final String run = runId("generic");
        final String sourceTopic = "SourceTest" + run;
        final String sinkTopic = "SinkTest" + run;
        final int messageCount = 10;

        LOG.info("Running basicFlowTest with sourceTopic={}, sinkTopic={}", sourceTopic, sinkTopic);
        createKafkaTopics(sourceTopic);

        try (Client subscriber = connectAmps("KafkaGenericSub" + run, AMPS_URI);
                Client publisher = connectAmps("KafkaGenericPub" + run, AMPS_URI)) {
            AMPSMessageHandler handler = subscribe(subscriber, sinkTopic, runFilter(run), messageCount);

            startConnector("amps-kafka-generic-sink-" + run, "AMPSKafkaGenericSink.json",
                    overrides(
                            "topics", sourceTopic,
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.TOPIC_CONFIG, sinkTopic,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "false"));
            startConnector("amps-kafka-generic-source-" + run, "AMPSKafkaGenericSource.json",
                    overrides(
                            Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                            Constants.TOPIC_CONFIG, sourceTopic,
                            Constants.MAX_BATCH_CONFIG, "10"));

            publishFixedLengthMessages(publisher, sourceTopic, run, messageCount);

            List<ReceivedMessage> received = takeMessages(handler, messageCount, MESSAGE_TIMEOUT_MS);
            assertSequences(received, run, messageCount);
            assertAllPayloadsHaveLength(received, 500);
        }
    }

    // Verifies regex topic subscription and sink publication using Kafka topic headers.
    @Test(timeout = 90000L)
    public void dynamicSubTest() throws Exception {
        final String run = runId("dynamic");
        final String sourceTopicPrefix = "Test" + run + "Source";
        final String sourceTopicOne = sourceTopicPrefix + "One";
        final String sourceTopicTwo = sourceTopicPrefix + "Two";
        final String sourceTopicRegex = sourceTopicPrefix + ".*";
        final String sinkTopicPrefix = "Test" + run + "Sink";
        final String sinkTopicOne = sinkTopicPrefix + "One";
        final String sinkTopicTwo = sinkTopicPrefix + "Two";
        final String sinkTopicRegex = sinkTopicPrefix + ".*";
        final int messageCount = 10;

        LOG.info("Running dynamicSubTest with sourceTopicRegex={}, sinkTopicRegex={}",
                sourceTopicRegex, sinkTopicRegex);
        createKafkaTopics(sinkTopicOne, sinkTopicTwo);

        try (Client subscriber = connectAmps("KafkaDynamicSub" + run, AMPS_URI);
                Client publisher = connectAmps("KafkaDynamicPub" + run, AMPS_URI)) {
            AMPSMessageHandler handler = subscribe(subscriber, sinkTopicRegex, runFilter(run), messageCount);

            startConnector("amps-kafka-dynamic-sink-" + run, "dynamicSub/AMPSKafkaDynamicSink.json",
                    overrides(
                            "topics.regex", sinkTopicRegex,
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "true"));
            startConnector("amps-kafka-dynamic-source-" + run, "dynamicSub/AMPSKafkaDynamicSource.json",
                    overrides(
                            Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                            Constants.TOPIC_CONFIG, sourceTopicRegex,
                            Constants.MAX_BATCH_CONFIG, "10",
                            "transforms", "route",
                            "transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter",
                            "transforms.route.regex", sourceTopicPrefix + "(.*)",
                            "transforms.route.replacement", sinkTopicPrefix + "$1"));

            publishJsonMessages(publisher, sourceTopicOne, run, 0, 5, 0L);
            publishJsonMessages(publisher, sourceTopicTwo, run, 5, 5, 0L);
            publisher.publishFlush(10000L);

            List<ReceivedMessage> received = takeMessages(handler, messageCount, MESSAGE_TIMEOUT_MS);
            assertSequences(received, run, messageCount);

            Set<String> receivedTopics = new HashSet<>();
            for (ReceivedMessage message : received) {
                receivedTopics.add(message.topic);
            }

            assertTrue(receivedTopics.contains(sinkTopicOne));
            assertTrue(receivedTopics.contains(sinkTopicTwo));
        }
    }

    // Verifies AMPS queue consumption through Kafka and back into an AMPS sink topic.
    @Test(timeout = 120000L)
    public void deliveryQueueTest() throws Exception {
        final String run = runId("queue");
        final String sinkTopic = "SinkTestQueue" + run;
        final int messageCount = 10;

        LOG.info("Running deliveryQueueTest with queueTopic={}, sinkTopic={}", QUEUE_TOPIC, sinkTopic);
        createKafkaTopics(QUEUE_TOPIC);

        try (Client subscriber = connectAmps("KafkaQueueSub" + run, AMPS_URI);
                Client publisher = connectAmps("KafkaQueuePub" + run, AMPS_URI)) {
            AMPSMessageHandler handler = subscribe(subscriber, sinkTopic, runFilter(run), messageCount);

            startConnector("amps-kafka-queue-sink-" + run, "AMPSKafkaGenericSink.json",
                    overrides(
                            "topics", QUEUE_TOPIC,
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.TOPIC_CONFIG, sinkTopic,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "false"));
            startConnector("amps-kafka-queue-source-" + run, "queueSub/AMPSKafkaQueueSource.json",
                    overrides(
                            Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                            Constants.TOPIC_CONFIG, QUEUE_TOPIC,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.ACK_BATCH_CONFIG, "5"));

            publishJsonMessages(publisher, QUEUE_TOPIC, run, 0, messageCount, 0L);
            publisher.publishFlush(10000L);

            List<ReceivedMessage> received = takeMessages(handler, messageCount, MESSAGE_TIMEOUT_MS);
            assertSequences(received, run, messageCount);
        }
    }

    // Verifies both memory and logged bookmark source configurations deliver replayable messages.
    @Test(timeout = 120000L)
    public void bookmarkTest() throws Exception {
        LOG.info("Running bookmarkTest with memory and logged bookmark sources");
        runBookmarkExample("bookmarkSub/AMPSKafkaMemoryBookmarkSource.json", "memoryBookmark");
        runBookmarkExample("bookmarkSub/AMPSKafkaLoggedBookmarkSource.json", "loggedBookmark");
    }

    // Verifies aggregate subscription filtering, grouping, projection, and sink delivery.
    @Test(timeout = 120000L)
    public void aggregateTest() throws Exception {
        final String run = runId("agg");
        final String sourceTopic = "Orders";
        final String sinkTopic = "AMPSKafkaSinkTest" + run;
        final int expectedUpdates = 10;

        LOG.info("Running aggregateTest with sourceTopic={}, sinkTopic={}", sourceTopic, sinkTopic);
        createKafkaTopics(sourceTopic);

        try (Client subscriber = connectAmps("KafkaAggSub" + run, AMPS_URI);
                Client publisher = connectAmps("KafkaAggPub" + run, AMPS_URI)) {
            AMPSMessageHandler handler = subscribe(subscriber, sinkTopic, runFilter(run), expectedUpdates);

            startConnector("amps-kafka-agg-sink-" + run, "aggSub/AMPSKafkaSink.json",
                    overrides(
                            "topics", sourceTopic,
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.TOPIC_CONFIG, sinkTopic,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "false"));
            startConnector("amps-kafka-agg-source-" + run, "aggSub/AMPSKafkaSource.json",
                    overrides(
                            Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                            Constants.FILTER_CONFIG, "(/run = '" + run + "') AND (/symbol IN ('IBM', 'MSFT'))",
                            Constants.OPTIONS_CONFIG, "projection=[/run,/symbol,avg(/price) as /avg_price, "
                                    + "avg(/price*/qty) as /avg_total],grouping=[/run,/symbol],conflation=1s",
                            Constants.SUB_ID_CONFIG, "Sub-" + run,
                            Constants.MAX_BATCH_CONFIG, "10"));

            for (String order : ExampleTestUtils.loadOrderMessagesWithRun("aggSub/messages.json", run)) {
                publisher.publish(sourceTopic, order);
                Thread.sleep(1100L);
            }
            publisher.publishFlush(10000L);

            List<ReceivedMessage> received = takeMessages(handler, expectedUpdates, MESSAGE_TIMEOUT_MS);
            assertEquals(expectedUpdates, received.size());
            for (ReceivedMessage message : received) {
                Map<String, Object> data = ExampleTestUtils.parseJson(message.data);
                assertEquals(run, data.get("run"));
                assertTrue("IBM".equals(data.get("symbol")) || "MSFT".equals(data.get("symbol")));
                assertTrue(data.containsKey("avg_price"));
                assertTrue(data.containsKey("avg_total"));
            }
        }
    }

    // Verifies HA source and sink connectors continue after the primary AMPS transport is disabled.
    @Test(timeout = 180000L)
    public void failoverTest() throws Exception {
        enableFailoverPrimaryTransport();
        requireAmps(AMPS_FAILOVER_PRIMARY_URI);
        requireAmps(AMPS_FAILOVER_SECONDARY_URI);

        final String run = runId("failover");
        final String sourceTopic = "SourceTest";
        final String sinkTopic = "SinkTestFailover" + run;
        final int messageCount = 60;

        LOG.info("Running failoverTest with primary={}, replica={}",
                AMPS_FAILOVER_PRIMARY_URI, AMPS_FAILOVER_SECONDARY_URI);
        createKafkaTopics(sourceTopic);

        HAClient subscriber = null;
        try {
            subscriber = newHAClient("KafkaFailoverSub" + run);
            subscriber.setBookmarkStore(new MemoryBookmarkStore());
            subscriber.connectAndLogon();
            AMPSMessageHandler handler = new AMPSMessageHandler(messageCount);
            Command command = new Command("subscribe")
                    .setTopic(sinkTopic)
                    .setFilter(runFilter(run))
                    .setBookmark(Client.Bookmarks.MOST_RECENT);
            subscriber.executeAsync(command, handler);

            startConnector("amps-kafka-failover-sink-" + run, "failoverTest/AMPSKafkaFailTestSink.json",
                    overrides(
                            "topics", sourceTopic,
                            Constants.URI_CONFIG, failoverUris(),
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.TOPIC_CONFIG, sinkTopic,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "false"));
            startConnector("amps-kafka-failover-source-" + run, "failoverTest/AMPSKafkaFailTestSource.json",
                    overrides(
                            Constants.URI_CONFIG, failoverUris(),
                            Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                            Constants.FILTER_CONFIG, runFilter(run),
                            Constants.SUB_ID_CONFIG, "Sub-" + run,
                            Constants.MAX_BATCH_CONFIG, "10"));

            AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
            Thread publisher = new Thread(() -> {
                try {
                    publishFailoverMessages(run, sourceTopic, messageCount, 100L);
                } catch (Throwable t) {
                    publisherFailure.set(t);
                }
            });
            publisher.start();

            List<ReceivedMessage> received = takeMessages(handler, 1, MESSAGE_TIMEOUT_MS);
            disableFailoverPrimaryTransport();

            publisher.join(MESSAGE_TIMEOUT_MS);
            if (publisher.isAlive()) {
                publisher.interrupt();
                fail("Failover publisher did not finish");
            }
            if (publisherFailure.get() != null) {
                throw new AssertionError("Failover publisher failed", publisherFailure.get());
            }

            received.addAll(takeMessages(handler, messageCount - received.size(), MESSAGE_TIMEOUT_MS));
            assertSequences(received, run, messageCount);
        } finally {
            try {
                enableFailoverPrimaryTransport();
            } finally {
                if (subscriber != null) {
                    subscriber.close();
                }
            }
        }
    }

    private void runBookmarkExample(String sourceExample, String namePrefix) throws Exception {
        final String run = runId(namePrefix);
        final String sourceTopic = "BookmarkSourceTest" + run;
        final String sinkTopic = "SinkTest" + run;
        final int messageCount = 10;

        LOG.info("Running bookmark source {} with sourceTopic={}, sinkTopic={}",
                sourceExample, sourceTopic, sinkTopic);
        createKafkaTopics(sourceTopic);

        Map<String, String> sourceOverrides = overrides(
                Constants.CLIENT_NAME_CONFIG, "KafkaSource" + run,
                Constants.TOPIC_CONFIG, sourceTopic,
                Constants.FILTER_CONFIG, runFilter(run),
                Constants.SUB_ID_CONFIG, "Sub-" + run,
                Constants.MAX_BATCH_CONFIG, "10");

        if (sourceExample.contains("Logged")) {
            Path bookmarkDir = ExampleTestUtils.PROJECT_ROOT.resolve("target/kafka-connect-test-bookmarks");
            Files.createDirectories(bookmarkDir);
            sourceOverrides.put(Constants.BOOKMARK_LOG_CONFIG,
                    bookmarkDir.resolve("bookmark-" + run + ".log").toString());
        }

        try (Client subscriber = connectAmps("KafkaBookmarkSub" + run, AMPS_URI);
                Client publisher = connectAmps("KafkaBookmarkPub" + run, AMPS_URI)) {
            AMPSMessageHandler handler = subscribe(subscriber, sinkTopic, runFilter(run), messageCount);

            startConnector("amps-kafka-bookmark-sink-" + run, "AMPSKafkaGenericSink.json",
                    overrides(
                            "topics", sourceTopic,
                            Constants.CLIENT_NAME_CONFIG, "KafkaSink" + run,
                            Constants.TOPIC_CONFIG, sinkTopic,
                            Constants.MAX_BATCH_CONFIG, "10",
                            Constants.PUBLISH_FLUSH_TIMEOUT_CONFIG, "10000",
                            Constants.USE_TOPIC_HEADER_CONFIG, "false"));
            startConnector("amps-kafka-bookmark-source-" + run, sourceExample, sourceOverrides);

            publishJsonMessages(publisher, sourceTopic, run, 0, messageCount, 0L);
            publisher.publishFlush(10000L);

            List<ReceivedMessage> received = takeMessages(handler, messageCount, MESSAGE_TIMEOUT_MS);
            assertSequences(received, run, messageCount);
        }
    }

    private void startConnector(String name, String examplePath, Map<String, String> overrides) throws Exception {
        Map<String, String> config = ExampleTestUtils.config(examplePath);
        config.putAll(overrides);
        if (!overrides.containsKey(Constants.URI_CONFIG)) {
            config.put(Constants.URI_CONFIG, AMPS_URI);
        }

        LOG.info("Starting connector {} from {}", name, examplePath);
        kafkaConnect.createConnector(name, config);
        connectorNames.add(name);
        kafkaConnect.waitForRunning(name, Integer.parseInt(config.get("tasks.max")), CONNECTOR_START_TIMEOUT_MS);
    }

    private static Client connectAmps(String clientName, String uri) throws Exception {
        Client client = new Client(clientName);
        client.connect(uri);
        client.logon();
        return client;
    }

    private static HAClient newHAClient(String clientName) {
        HAClient client = new HAClient(clientName);
        client.setServerChooser(failoverServerChooser());
        return client;
    }

    private static DefaultServerChooser failoverServerChooser() {
        DefaultServerChooser chooser = new DefaultServerChooser();
        chooser.add(AMPS_FAILOVER_PRIMARY_URI);
        chooser.add(AMPS_FAILOVER_SECONDARY_URI);
        return chooser;
    }

    private static String failoverUris() {
        return AMPS_FAILOVER_PRIMARY_URI + "," + AMPS_FAILOVER_SECONDARY_URI;
    }

    private static AMPSMessageHandler subscribe(Client client, String topic, String filter, int capacity)
            throws Exception {
        AMPSMessageHandler handler = new AMPSMessageHandler(capacity);
        client.subscribe(handler, topic, filter, 0);
        return handler;
    }

    private static void publishFixedLengthMessages(Client publisher, String topic, String run, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            publisher.publish(topic, ExampleTestUtils.fixedLengthJsonMessage(run, i, 500));
        }
        publisher.publishFlush(10000L);
    }

    private static void publishJsonMessages(Client publisher, String topic, String run, int startSequence,
            int count, long delayMillis) throws Exception {
        for (int i = 0; i < count; i++) {
            publisher.publish(topic, ExampleTestUtils.jsonMessage(run, startSequence + i));
            if (delayMillis > 0L) {
                Thread.sleep(delayMillis);
            }
        }
    }

    private static void publishFailoverMessages(String run, String topic, int count, long delayMillis)
            throws Exception {
        HAClient publisher = null;
        try {
            publisher = newHAClient("KafkaFailoverPub" + run);
            publisher.setPublishStore(new MemoryPublishStore(1000));
            publisher.connectAndLogon();
            publishJsonMessages(publisher, topic, run, 0, count, delayMillis);
            publisher.publishFlush(10000L);
        } finally {
            if (publisher != null) {
                publisher.close();
            }
        }
    }

    private static List<ReceivedMessage> takeMessages(AMPSMessageHandler handler, int expected, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<ReceivedMessage> received = new ArrayList<>();

        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            long waitMillis = Math.max(1L, Math.min(500L, deadline - System.currentTimeMillis()));
            ReceivedMessage message = handler.messages.poll(waitMillis, TimeUnit.MILLISECONDS);
            if (message != null) {
                received.add(message);
            }
        }

        assertEquals("Timed out waiting for AMPS messages", expected, received.size());
        return received;
    }

    private static void assertSequences(List<ReceivedMessage> messages, String run, int expected) throws Exception {
        Set<Integer> sequences = new HashSet<>();

        for (ReceivedMessage message : messages) {
            Map<String, Object> data = ExampleTestUtils.parseJson(message.data);
            assertEquals(run, data.get("run"));
            assertNotNull(data.get("seq"));
            sequences.add(((Number) data.get("seq")).intValue());
        }

        assertEquals(expected, sequences.size());
        for (int i = 0; i < expected; i++) {
            assertTrue("Missing sequence " + i, sequences.contains(i));
        }
    }

    private static void assertAllPayloadsHaveLength(List<ReceivedMessage> messages, int expectedLength) {
        for (ReceivedMessage message : messages) {
            assertEquals(expectedLength, message.data.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    private static String runFilter(String run) {
        return "/run = '" + run + "'";
    }

    private static String runId(String prefix) {
        return prefix + Long.toHexString(System.nanoTime());
    }

    private static Map<String, String> overrides(String... values) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            overrides.put(values[i], values[i + 1]);
        }
        return overrides;
    }

    private static void requireAmps(String uri) throws Exception {
        try (Client client = connectAmps("KafkaExampleProbe" + Long.toHexString(System.nanoTime()), uri)) {
            // Connection and logon are the health check.
        }
    }

    private static void requireKafka() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaProperties())) {
            admin.listTopics().names().get(10L, TimeUnit.SECONDS);
        }
    }

    private static void createKafkaTopics(String... topics) throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaProperties())) {
            Set<String> existing = admin.listTopics().names().get(10L, TimeUnit.SECONDS);
            List<NewTopic> missing = new ArrayList<>();
            for (String topic : topics) {
                if (!existing.contains(topic)) {
                    missing.add(new NewTopic(topic, 1, (short) 1));
                }
            }

            if (!missing.isEmpty()) {
                try {
                    admin.createTopics(missing).all().get(10L, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                }
            }
        }
    }

    private static Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }

    private static void enableFailoverPrimaryTransport() throws Exception {
        publishTransportAction(ENABLE_FAILOVER_TRANSPORT_TOPIC);
    }

    private static void disableFailoverPrimaryTransport() throws Exception {
        publishTransportAction(DISABLE_FAILOVER_TRANSPORT_TOPIC);
    }

    private static void publishTransportAction(String topic) throws Exception {
        LOG.info("Publishing AMPS transport action {}", topic);
        try (Client client = connectAmps("KafkaFailoverTransport"
                + Long.toHexString(System.nanoTime()), AMPS_URI)) {
            client.publish(topic, "{}");
            client.publishFlush(10000L);
        }
    }
}
