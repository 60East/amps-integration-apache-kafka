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

import java.util.HashMap;
import java.util.Map;
import java.nio.charset.Charset;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.Store;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.FailedWriteHandler;
import com.crankuptheamps.client.exception.DisconnectedException;
import com.crankuptheamps.client.fields.ReasonField;
import com.crankuptheamps.client.util.SerializableFunction;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import static com.crankuptheamps.kafka.Constants.*;

public class AMPSKafkaSinkTask extends SinkTask {
    
    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AMPSKafkaSink.class);

    /**
     * The AMPS client instance.
     */
    protected Client client;

    /**
     * The AMPS publish store instance.
     */
    protected Store pStore = null;

    /**
     * The AMPS configuration definition.
     */
    protected AMPSKafkaSinkConfig config;

    /**
     * The default configured AMPS topic that events should be published to.
     */
    protected String topic;

    /**
     * A binary array of bytes representing the default configured topic.
     */
    protected byte[] topicBytes;

    /**
     * The maximum number of records to read from the channel for a transaction.
     */
    protected int batchSize;

    /**
     * Indicates whether we should publish records to the topic specified in
     * their topic header, instead of the sink's default configured topic.
     */
    protected boolean useTopicHeader;

    /**
     * Timeout we should use for publish flush at the end of the batch.
     * A -1 indicates not to publish flush at end of batch.
     */
    protected long publishFlushTimeout;

    /**
     * Indicates if we should log the message to the Kafka log when notified
     * that it is a failed write.
     */
    protected boolean logMsgOnWriteError;

    /**
     * Cache of string topic names mapped to their binary byte array
     * representation used for publishing to AMPS. This is only used when
     * useTopicHeader is true. It caches all the topics we've seen so far.
     */
    protected Map<String, byte[]> topicCache;

    /**
     * Indicates whether our AMPS client is current connected or disconnected,
     * as reported by our connection state listener.
     */
    protected volatile boolean connected;

    /**
     * Indicates whether our AMPS client is shutdown,
     * as reported by our connection state listener.
     */
    protected volatile boolean shutdown;

    /**
     * The charset to use for encoding messages.
     */
    protected Charset charset;

    /**
     * Used for debugging the messages per second.
     */
    int batchCount = 0;
    int count = 0;
    long now = 0L;
    long start = 0L;

    /**
     * Failed write handler we register on the client to detect publishing
     * errors during the batch.
     */
    public class SinkFailedWriteHandler implements FailedWriteHandler {
        @Override
        public void failedWrite(Message message, int reason) {
            String msg;
            if (logMsgOnWriteError) {
                msg = message.toString();
            } else {
                msg = "{msg seq " + message.getSequence() + "}";
            }
            LOGGER.error("FAILED WRITE: reason = {}, msg = {}",
                    ReasonField.encodeReason(reason), msg);
        }
    }

    /**
     * Connection state listener we register on the client to detect disconnects
     * and reconnects.
     */
    public class SinkConnectionListener implements ConnectionStateListener {
        @Override
        public void connectionStateChanged(int newState) {
            String state = "UNKNOWN: " + newState;
            switch (newState)
            {
                case ConnectionStateListener.Connected:
                    state = "Connected";
                    shutdown = false;
                    break;
                case ConnectionStateListener.LoggedOn:
                    state = "LoggedOn";
                    break;
                case ConnectionStateListener.PublishReplayed:
                    state = "PublishReplayed";
                    break;
                case ConnectionStateListener.HeartbeatInitiated:
                    state = "HeartbeatInitiated";
                    break;
                case ConnectionStateListener.Resubscribed:
                    state = "Resubscribed";
                    connected = true;
                    break;
                case ConnectionStateListener.Disconnected:
                    state = "Disconnected";
                    connected = false;
                    break;
                case ConnectionStateListener.Shutdown:
                    state = "Shutdown";
                    connected = false;
                    shutdown = true;
                    break;
            }
            LOGGER.info("SinkConnectionListener: connection state changed: "
                    + state);
        }
    }

    /**
     * @return version of connector
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Setup the AMPS client and message handler using defined configuration.
     */
    @Override
    public void start(Map<String, String> properties) {
        LOGGER.debug("AMPSKafkaSinkTask is starting -----------------------------------------------------------------------------");
        this.config = new AMPSKafkaSinkConfig(properties);
        try {
            String clientFactoryClass = config.getString(CLIENT_FACTORY_CONFIG);
            Class factoryClass = Class.forName(clientFactoryClass);
            @SuppressWarnings("unchecked")
            SerializableFunction<AbstractConfig, Client> clientFactory =
                    (SerializableFunction<AbstractConfig, Client>) factoryClass.getDeclaredConstructor().newInstance();

            // Create the AMPS client.
            client = clientFactory.apply(config);

            // Get the publish store.
            pStore = client.getPublishStore();

            // Set failed write handler.
            client.setFailedWriteHandler(new SinkFailedWriteHandler());

            // Set connection state listener.
            client.addConnectionStateListener(new SinkConnectionListener());

            // Get the default topic we publish to.
            topic = config.getString(TOPIC_CONFIG);
            topicBytes = topic.getBytes();

            // Get the batch size we'll write before publish flushing and
            // committing the transaction.
            batchSize = config.getInt(MAX_BATCH_CONFIG);

            // Should we publish events to the topic specified in their topic
            // header, or always use the sink's default configured topic?
            useTopicHeader = config.getBoolean(USE_TOPIC_HEADER_CONFIG);
            if (useTopicHeader) {
                topicCache = new HashMap<>();
                topicCache.put(topic, topicBytes);
            }

            // Timeout we should use for publish flush at the end of the batch.
            // A -1 indicates not to publish flush at end of batch.
            publishFlushTimeout = config.getLong(PUBLISH_FLUSH_TIMEOUT_CONFIG);

            // Indicates if we should log the message to the Kafka log when notified
            // that it is a failed write.
            logMsgOnWriteError = config.getBoolean(LOG_MSG_ON_WRITE_ERROR_CONFIG);

            // Connect and logon to AMPS server.
            ((HAClient)client).connectAndLogon(); 

            // Set the charset for Sink processing
            this.charset = client.allocateMessage().getEncoder().charset();

        } catch (Exception e) {
            throw new KafkaException(
                    "AMPS Kafka Sink failed to start due to: " + e, e);
        } finally {
            LOGGER.debug("Leaving start() ---------------------------------");
        }
    }
    
    /**
     * Takes records from kafka and publishes each as an AMPS message.
     */
    @Override
    public void put(Collection<SinkRecord> records) {    
        LOGGER.debug("Begin put() --------------------------");
        try {
            for (SinkRecord record : records) {
                // Grab a record from the collection of records.
                if (record == null) {
                    // if null, no records need to be passed
                    break;
                }

                // Get the topic.
                String tpc = topic;
                byte[] tpcBytes = topicBytes;
                if (useTopicHeader) {
                    tpc = record.topic();
                    tpcBytes = topicCache.get(tpc);
                    if (tpcBytes == null) {
                        tpcBytes = tpc.getBytes();
                        topicCache.put(tpc, tpcBytes);
                    }
                }

                Object recordValue = record.value();
                byte[] value = null;

                if (recordValue instanceof byte[]) {
                    value = (byte[]) recordValue;
                } else if (recordValue instanceof String) {
                    value = ((String) recordValue).getBytes(this.charset);
                } else if (recordValue == null) {
                    LOGGER.warn("Received null value for record in topic {}", record.topic());
                } else {
                    LOGGER.error("Unsupported record type: {}. Please use ByteArrayConverter or StringConverter.", 
                                recordValue.getClass().getName());
                }

                // Publish the event as an AMPS message.
                boolean shouldRetry = true;
                while (shouldRetry) {
                    try {
                        batchCount++;
                        count++;
                        client.publish(tpcBytes, 0, tpcBytes.length, value, 0, value.length);
                        shouldRetry = false;

                        if (start == 0L) {
                            start = System.currentTimeMillis();
                        }
        
                        if ( (count % 5000 == 0) && ((now = System.currentTimeMillis()) - start > 1000L) ) {
                            LOGGER.debug(String.format("COUNT = %d, batchCount = %d, records.size=%d, Messages per second = %,.2f", count, batchCount, records.size(), ((double) batchCount) / (double) (now - start) * 1000.0D));
                            batchCount = 0;
                            start = now;
                        }
                    } catch (DisconnectedException e) {
                        if (pStore != null) {
                            shouldRetry = false; 
                        }
                        
                        // Wait until we are reconnected to continue publishing,
                        // unless client is shutdown.
                        while (!connected && !shutdown) {
                            Thread.sleep(1000L);
                        }
                        if (shutdown) {
                            // Rethrow
                            throw e;
                        }
                    }
                }
            }

            // Wait for the batch to flush to AMPS if specified by config.
            if (publishFlushTimeout != -1L) {
                client.publishFlush(publishFlushTimeout);
            }

        } catch (Exception e) {
            LOGGER.error("An error occurred while processing batch: " + e, e);
        } finally {
            LOGGER.debug("Leaving put() --------------------------");
        }

    }

    /*
     *  Close the AMPS client
     */
    @Override
    public void stop() {
        LOGGER.debug("Entering stop() -------------------------------------");
        try {
            topicCache = null;
            if (client != null) {
                client.close();
            }

            if (pStore != null) {
                pStore.close();
            }
            
        } catch (Exception e) {
            throw new KafkaException(
                    "AMPS Kafka Sink failed to stop due to: " + e, e);
        } finally {
            LOGGER.debug("Leaving stop() ----------------------------------");
        }
    }
}
