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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.fields.CommandField;
import com.crankuptheamps.client.fields.Field;
import com.crankuptheamps.client.util.SerializableFunction;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.clients.producer.RecordMetadata;

import static com.crankuptheamps.kafka.Constants.*;

/**
 * The logic behind each task, pulling messages from AMPS,
 * and passing it on to Kafka connect.
 */
public class AMPSKafkaSourceTask extends SourceTask{

    /**
     * Logger for this class.
     */
    private static Logger LOGGER = LoggerFactory.getLogger(AMPSKafkaSourceTask.class);
    
    /**
     * The AMPS client instance.
     */
    protected Client client;

    /**
     * The AMPS bookmark store instance.
     */
    protected BookmarkStore bStore = null;

    /** 
    * Flag that indicates whether we are performing bookmark
    * store discards for a bookmark subscription.
    */
    protected boolean doDiscards;

    /**
     * Flag that indicates if we are subscribed to a queue and
     * need to acknowledge messages.
     */
    protected boolean isQueue;

    /**
     * Helper class of message buffers that the AMPS asynchronous
     * {@link MessageHandler} writes messages to for batching into Kafka
     * source records.
     */
    protected MessageMultiBuffer msgMultiBuf;

    /**
     * Helper class that implements {@link MessageHandler} for feeding
     * our asynchronous message stream into the {@link MessageMultiBuffer}.
     */
    protected KafkaSrcMsgHandler handler;

    /**
     * The command ID of the AMPS subscription.
     */
    protected CommandId cmdId;

    /**
     * The subscription ID used for discarding messages in the bookmark store.
     */
    protected Field subId;

    /**
     * A bit flag mask used to filter out incoming messages based upon
     * their command type integer. Obtained from the configuration via
     * the {@link CMD_TYPE_FILTER_CONFIG} config key.
     */
    protected int cmdTypeFilter;

    /**
     * The current batch of messages retrieved from the message multi-buffer.
     */
    protected List<AMPSSourceRecord> messages = null;

    /**
     * When the configured AMPS client is using a LoggedBookmarkStore,
     * the AMPS Kafka source will prune it of old entries whenever
     * it is polled and it finds that at least this many milliseconds
     * have elapsed since the last prune operation or startup. The default is
     * 5 minutes (300,000 milliseconds). Pruning will also be done upon source
     * shutdown.
     * 
     * If this parameter is set to zero, both periodic and shutdown
     * pruning will be turned off. This may be useful for testing and debugging,
     * but is NOT recommended for a production deployment.
     */
    protected long pruneTimeMillis;

    /**
     * Timestamp of when this source last performed a prune operation, or
     * when it finished start-up if no prune operations have occurred.
     */
    protected long lastPruneMillis;

    /**
     * The set of keys for the event headers that should be preserved on to each record.
     */
    protected Set<String> headerKeys;

    /**
     * The source connector configuration definition.
     */
    protected AMPSKafkaSourceConfig config;

    /**
     * Polls AMPS for messages and sends them to Kafka in the form of source records.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOGGER.debug("Entering poll() in AMPSKafkaSourceTask ------------------------------------------------------------------------");
        List<SourceRecord> records = null;
        boolean error = true;
        int size = 0;
        messages = null;

        try {
            messages = msgMultiBuf.rotateBuffer();
            size = messages.size();
            LOGGER.debug("Batch size: {}", size);

            // If the batch is empty return null
            if (size == 0) {
                error = false;
                LOGGER.debug("Returning null for empty batch...");
                return null;
            }

            // Add the batch to a fresh list that can be returned to Kafka.
            records = new ArrayList<>();
            records.addAll(messages);

            error = false;
            LOGGER.debug("Returning batch for processing by Kafka.");
            return records;

        } catch (Exception e) {
            LOGGER.error("An unexpected error occurred.", e);
        } finally {
            if (error) {
                if (messages != null)
                    msgMultiBuf.pushBack(messages);
                LOGGER.debug("Rolled-back batch for redelivery.");

            }
            // Prune logged bookmark store if enough time has elapsed
            try {
                long currTime = System.currentTimeMillis();
                if (bStore instanceof LoggedBookmarkStore
                        && pruneTimeMillis != 0L
                        && (currTime - lastPruneMillis) > pruneTimeMillis) {
                    ((LoggedBookmarkStore) bStore).prune();
                    lastPruneMillis = currTime;
                }
            } catch (Exception e) {
                LOGGER.error("Bookmark store prune operation failed.", e);
            }
            // Return the buffer to the multi-buffer since we're done with it
            msgMultiBuf.returnBuffer(messages);
            LOGGER.debug("Leaving poll() in AMPSKafkaSourceTask ------------------------------------------------------------------------");
        }
        return null;
    }

    /**
     * Kafka calls this for a source record once it's been persisted within
     * Kafka, so "offsets" can be updated in the source system. This is the
     * proper place to discard an AMPS message from a bookmark subscription,
     * or ack a message from a message queue.
     * 
     * @param record The source record that was just persisted to Kafka
     *        and can be acknowledged/discarded in the AMPS subscription. 
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        LOGGER.debug("Entering commitRecord() in AMPSKafkaSourceTask ------------------------------------------------------------------------");
        try {
            LOGGER.trace("SourceRecord: {}", record);
            // do discards on the processed messages
            if (doDiscards) {
                // This should be an AMPSSourceRecord.
                AMPSSourceRecord rec = (AMPSSourceRecord) record;
                bStore.discard(subId, rec.getBookmarkSeqNo());
            }
            // acknowledge processed messages 
            if (isQueue) {
                AMPSSourceRecord rec = (AMPSSourceRecord) record;
                client.ack(rec.getTopic(), rec.getBookmark());
            }
        } catch (Exception e) {
            LOGGER.error("Message discard/acknowledgement failed: record = " + record, e);
        } finally {
            LOGGER.debug("Leaving commitRecord() in AMPSKafkaSourceTask ------------------------------------------------------------------------");
        }
    }

    /**
     * Based upon configuration from the AMPSKafkaSource configuration, starts up an AMPS
     * Client and associated subscription.
     */
    @Override
    public void start(Map<String, String> properties) {
        LOGGER.debug("Starting AMPSKafkaSourceTask Start() ------------------------------------------------------------------");
        // copy configuration definition
        this.config = new AMPSKafkaSourceConfig(properties);

        try {
            pruneTimeMillis = config.getLong(PRUNE_TIME_THRESHOLD_CONFIG);
            String clientFactoryClass = config.getString(CLIENT_FACTORY_CONFIG);
            Class factoryClass = Class.forName(clientFactoryClass);
            @SuppressWarnings("unchecked")
            SerializableFunction<AbstractConfig, Client> clientFactory =
                (SerializableFunction<AbstractConfig, Client>) factoryClass.getDeclaredConstructor().newInstance();

            // create the AMPS client
            client = clientFactory.apply(config);

            // Connect and logon to AMPS server.
            ((HAClient)client).connectAndLogon();

            // get the client's bookmark store
            bStore = client.getBookmarkStore();

            // get the necessary parameters
            String cmdType = config.getString(COMMAND_CONFIG);
            String topic = config.getString(TOPIC_CONFIG);
            String filter = config.getString(FILTER_CONFIG);
            String options = config.getString(OPTIONS_CONFIG);
            String bookmark = config.getString(BOOKMARK_CONFIG);
            String bookmarkStore = config.getString(BOOKMARK_STORE_CONFIG);
            String subId = config.getString(SUB_ID_CONFIG);
            String evtHeaders = config.getString(EVENT_HEADERS_CONFIG); 
            String queue = config.getString(QUEUE_CONFIG);
            String maxBacklog = "max_backlog=" + config.getString(MAX_BACKLOG_CONFIG);                      
            int maxBuffs = config.getInt(MAX_BUFFS_CONFIG);
            int maxBatch = config.getInt(MAX_BATCH_CONFIG);

            if (bookmark != null && !bookmark.isEmpty()
                && (subId == null || subId.isEmpty()))
                throw new IllegalArgumentException("Subscription ID must be "
                    + "set when using a bookmark subscription.");

            if (subId != null && !subId.isEmpty()) {
                this.subId = new Field(subId);
            }

            LOGGER.debug("maxBuffs: {}", maxBuffs);
            LOGGER.debug("maxBatch: {}", maxBatch);
            msgMultiBuf = new MessageMultiBuffer(maxBuffs, maxBatch);
            handler = new KafkaSrcMsgHandler(msgMultiBuf);
            LOGGER.debug("command: {}", cmdType);
            LOGGER.debug("topic: {}", topic);
            Command command = new Command(cmdType).setTopic(topic);

            // If there is a filter, set it
            if (filter != null && !filter.isEmpty()) {
                command.setFilter(filter);
                LOGGER.debug("filter: {}", filter);
            }

            // Determine if we are dealing with a queue
            isQueue = "y".equalsIgnoreCase(queue);

            if (isQueue) {
                // If it's a queue, append backlog to options, if options already exist
                String opts = (options != null && !options.isEmpty()) 
                            ? options + "," + maxBacklog 
                            : maxBacklog;
                
                command.setOptions(opts);
                LOGGER.debug("Options: {}", opts);
            } else if (options != null && !options.isEmpty()) {
                // Not a queue, so just set the standard options if provided
                command.setOptions(options);
                LOGGER.debug("Options: {}", options);
            }

            // If there is a bookmark subscription, set
            if (subId != null && !subId.isEmpty()) {
                command.setSubId(subId);
                LOGGER.debug("subId: {}", subId);
                if (bookmark != null && !bookmark.isEmpty()) {
                    if ("NOW".equalsIgnoreCase(bookmark)) {
                        command.setBookmark(Client.Bookmarks.NOW);
                    } else if ("EPOCH".equalsIgnoreCase(bookmark)) {
                        command.setBookmark(Client.Bookmarks.EPOCH);
                    } else if ("MOST_RECENT".equalsIgnoreCase(bookmark)) {
                        command.setBookmark(Client.Bookmarks.MOST_RECENT);
                    } else {
                        command.setBookmark(bookmark);
                    }
                    LOGGER.debug("bookmark: {}", bookmark);
                }

                // if there is a store, do discards
                if (bookmarkStore != null && !bookmarkStore.isEmpty()) {
                    doDiscards = true;
                    LOGGER.debug("bookmarkStore: {}", bookmarkStore);
                }
            }

            // Set the message command type filter
            cmdTypeFilter = Message.Command.Publish | Message.Command.SOW | Message.Command.DeltaPublish | Message.Command.OOF;
            String cmdTypeFilterStr = config.getString(CMD_TYPE_FILTER_CONFIG);
            if (cmdTypeFilterStr != null && !cmdTypeFilterStr.isEmpty()) {
                cmdTypeFilter = Integer.parseInt(cmdTypeFilterStr);
                LOGGER.debug("cmdTypeFilter: {}", cmdTypeFilter);
            }
            
            // Are any headers specified that should be set on each event?
            if (evtHeaders != null && !evtHeaders.isEmpty()) {
                LOGGER.debug("event headers: {}", evtHeaders);
                Set<String> knownKeys = new HashSet<String>();
                knownKeys.add(COMMAND_HEADER_CONFIG);
                knownKeys.add(TOPIC_HEADER_CONFIG);
                knownKeys.add(SOW_KEY_HEADER_CONFIG);
                knownKeys.add(AMPS_TIMESTAMP_HEADER_CONFIG);
                knownKeys.add(BOOKMARK_HEADER_CONFIG);
                knownKeys.add(CORRELATION_ID_HEADER_CONFIG);
                knownKeys.add(SUB_ID_HEADER_CONFIG);
                knownKeys.add(LENGTH_HEADER_CONFIG);
                knownKeys.add(TIMESTAMP_HEADER_CONFIG);
                headerKeys = new HashSet<String>();
                String[] hdrKeys = evtHeaders.split(",");
                for (String key: hdrKeys) {
                    key = key.trim();
                    if (!key.isEmpty()) {
                        headerKeys.add(key);
                    }
                }
                Set<String> badKeys = new HashSet<String>(headerKeys);
                badKeys.removeAll(knownKeys);
                if (!badKeys.isEmpty()) {
                    throw new IllegalArgumentException("Invalid event header "
                            + "key(s): " + badKeys);
                }
            }

            // Execute the command
            cmdId = client.executeAsync(command, handler);

        } catch (Exception e) {
            throw new KafkaException("AMPS Kafka Source failed to start due to " + e, e);
        } finally {
            lastPruneMillis = System.currentTimeMillis();
            LOGGER.debug("Leaving Start() in AMPSKafkaSourceTask ------------------------------------------------------------------");
        }

    }
    
    /**
     * Shuts-down the AMPS subscription and client.
     */
    @Override
    public void stop() {
        LOGGER.debug("Stopping AMPSKafkaSourceTask --------------------------------------------------------------------------------");
        try {
            if (client != null) {
                client.unsubscribe();
                client.close();
            }

            if (bStore != null) {
                if (bStore instanceof LoggedBookmarkStore) {
                    if (pruneTimeMillis > 0L) {
                        ((LoggedBookmarkStore) bStore).prune();
                    }
                }
                bStore.close();
            }

        } catch (Exception e) {
            throw new KafkaException("AMPS Kafka Source failed to stop due to " + e, e);
        } finally {
            LOGGER.debug("Leaving Stop() in AMPSKafkaSourceTask ------------------------------------------------------------------");
        }
    }

    /**
     * Returns the connector version.
     */
    @Override
    public String version() {
        return Version.getVersion();
    }


    /**
     * Helper class that implements {@link MessageHandler} for feeding
     * our asynchronous message stream into the {@link MessageMultiBuffer}.
     */
    protected class KafkaSrcMsgHandler implements MessageHandler {
        private MessageMultiBuffer msgMultiBuf;

        /**
         * The topic for publishing.
         */
        private String topic = config.getString(TOPIC_CONFIG);

        /**
         * The record key.
         */
        private String recordKey = config.getString(KAFKA_KEY_CONFIG);

        /**
         * Timestamp and partition, which are unused for now.
         */
        private Long timestamp = null;
        private Integer partition = null;

        /**
         * The source partition and offset used to create Kafka source records.
         */
        private final Map<String, ?> sourcePartition = new HashMap<>();
	    private final Map<String, ?> sourceOffset = new HashMap<>();
        
        /**
         * Constructs a new Kafka source message handler with the specified
         * message multi-buffer.
         * 
         * @param mmb The message multi-buffer to copy the asynchronous message
         *        stream into for Kafka batching.
         */
        public KafkaSrcMsgHandler(MessageMultiBuffer mmb) {
            msgMultiBuf = mmb;
        }

        /**
         * Invoked by AMPS Client background reader thread for asynchronous
         * message delivery.
         */
        @Override
        public void invoke(Message msg) {

            // Filter out unwanted command types.
            if ((msg.getCommand() & cmdTypeFilter) == 0) {
                LOGGER.debug("Filtering message out of source by command type: "
                        + "cmdType={}, cmdTypeFilter={}",
                        msg.getCommand(), cmdTypeFilter);
                return;
            }
            
            // Obtain the message value as a byte array
            Field data = msg.getDataRaw();
            byte[] ba = new byte[data.length];
            System.arraycopy(data.buffer, data.position, ba, 0, data.length);

            // Get the exact topic for the message
            String msgTopic = msg.getTopic();

            // Partition, timestamp, and headers are null for now
            AMPSSourceRecord record = new AMPSSourceRecord(sourcePartition, sourceOffset, msgTopic,
                partition, Schema.STRING_SCHEMA, recordKey, Schema.BYTES_SCHEMA, ba, timestamp, null);

            // If we have headers defined in the config, pass them to the Kafka record
            if (headerKeys != null && !headerKeys.isEmpty()) {
                addEventHeaders(record, msg);
            }

            // if its a queue, set bookmark and topic so we can ack after record is persisted in Kafka
            if (isQueue) {
                record.setBookmark(msg.getBookmark());
                record.setTopic(msgTopic);
            }

            record.setBookmarkSeqNo(msg.getBookmarkSeqNo());
            msgMultiBuf.append(record);
        }
        
        /**
         * Add any specified event headers to the message event.
         */
        protected void addEventHeaders(AMPSSourceRecord record, Message msg) {
            Headers hdrs = record.headers();
            if (headerKeys.contains(COMMAND_HEADER_CONFIG)) {
                hdrs.add(COMMAND_HEADER_CONFIG,
                        CommandField.encodeCommand(msg.getCommand()), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(TOPIC_HEADER_CONFIG)) {
                hdrs.add(TOPIC_HEADER_CONFIG, msg.getTopic(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(SOW_KEY_HEADER_CONFIG)) {
                hdrs.add(SOW_KEY_HEADER_CONFIG, msg.getSowKey(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(AMPS_TIMESTAMP_HEADER_CONFIG)) {
                hdrs.add(AMPS_TIMESTAMP_HEADER_CONFIG, msg.getTimestamp(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(BOOKMARK_HEADER_CONFIG)) {
                hdrs.add(BOOKMARK_HEADER_CONFIG, msg.getBookmark(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(CORRELATION_ID_HEADER_CONFIG)) {
                hdrs.add(CORRELATION_ID_HEADER_CONFIG, msg.getCorrelationId(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(SUB_ID_HEADER_CONFIG)) {
                hdrs.add(SUB_ID_HEADER_CONFIG, msg.getSubId(), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(LENGTH_HEADER_CONFIG)) {
                hdrs.add(LENGTH_HEADER_CONFIG, String.valueOf(msg.getLength()), Schema.STRING_SCHEMA);
            }
            if (headerKeys.contains(TIMESTAMP_HEADER_CONFIG)) {
                hdrs.add(TIMESTAMP_HEADER_CONFIG,
                        String.valueOf(System.currentTimeMillis()), Schema.STRING_SCHEMA);
            }
        }
    }

    /**
     * A simple message class to copy the important parts of the AMPS message
     * to. This allows us to copy the minimal amount of data into the
     * MessageMultiBuffer so that this Kafka source takes less memory and
     * doesn't waste time copying message fields it doesn't need.
     */
    protected static class AMPSSourceRecord extends SourceRecord {
        /**
         * The bookmark sequence number assigned to this message when it was
         * logged into the bookmark store. We'll need this later for discarding
         * the message.
         */
        private long bookmarkSeqNo;

        /**
         * The bookmark associated with the message. Used for acking messages
         * in a queue.
         */
        private String bookmark;

        /**
         * The topic associated with the message. Used for acking messages
         * in a queue.
         */
        private String topic;

        /**
         * The constructor calling for the SourceRecord constructor.
         */
        public AMPSSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                                String topic, Integer partition,
                                Schema keySchema, Object key,
                                Schema valueSchema, Object value,
                                Long timestamp, Iterable<Header> headers) {
            super(sourcePartition, sourceOffset, topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
        }
        
        /**
         * Get the bookmark store sequence number used to discard the message.
         */
        public long getBookmarkSeqNo() {
            return bookmarkSeqNo;
        }
        
        /**
         * Set the bookmark store sequence number used to discard the message.
         */
        public void setBookmarkSeqNo(long bmSeqNo) {
            bookmarkSeqNo = bmSeqNo;
        }

        /**
         * Get the bookmark used to ack a message.
         */
        public String getBookmark() {
            return bookmark;
        }
        
        /**
         * Set the bookmark used to ack a message.
         */
        public void setBookmark(String bm) {
            bookmark = bm;
        }

        /**
         * Get the topic used to ack a message.
         */
        public String getTopic() {
            return topic;
        }
        
        /**
         * Set the topic used to ack a message.
         */
        public void setTopic(String tpc) {
            topic = tpc;
        }
        
    }

    /**
     * Helper class of message buffers that the AMPS asynchronous
     * {@link MessageHandler} writes messages to for batching into Kafka
     * source records.
     */
    protected static class MessageMultiBuffer {
        private Lock lock;
        private Condition isEmpty;
        private Condition isFull;
        private int maxBuffs;
        private int maxBatch;
        private List<List<AMPSSourceRecord>> msgBuffers;
        private List<AMPSSourceRecord> currBuffer;
        private List<AMPSSourceRecord> rollbackBuffer;
        private Deque<List<AMPSSourceRecord>> stack = new ArrayDeque<List<AMPSSourceRecord>>();
        private boolean rolledback;
        private int readIndex;
        private int writeIndex;
        
        /**
         * Constructs an instance with a maximum number of message buffers
         * and maximum batch size.
         * 
         * @param bufs Max number of buffers.
         * @param batch Max batch size. This shouldn't be larger than your
         *        smallest configured transaction capacity for Kafka.
         */
        public MessageMultiBuffer(int bufs, int batch) {
            lock = new ReentrantLock();
            isEmpty = lock.newCondition();
            isFull = lock.newCondition();
            maxBuffs = bufs;
            maxBatch = batch;
            msgBuffers = new ArrayList<List<AMPSSourceRecord>>();
            for (int i = 0; i < maxBuffs; i++) {
                msgBuffers.add(new LinkedList<AMPSSourceRecord>());
            }
            rollbackBuffer = new LinkedList<AMPSSourceRecord>();
            rolledback = false;
            readIndex = writeIndex = 0;
            currBuffer = msgBuffers.get(writeIndex);
        }
        
        /**
         * Called by the AMPS asynchronous message handler to write a message
         * into the multi-buffer.
         */
        public void append(AMPSSourceRecord msg) {
            lock.lock();
            try {
                if (currBuffer.size() == maxBatch) {
                    int newWriteIdx = writeIndex + 1;
                    if (newWriteIdx == maxBuffs) newWriteIdx = 0;
                    while (currBuffer.size() == maxBatch
                            && newWriteIdx == readIndex) {
                        LOGGER.warn("Pausing AMPS message processing to let Kafka "
                                + "source polling catch-up. Consider increasing "
                                + "maxBuffers; or maxBatch and the max tasks"
                                + "of your Kafka configuration.");
                        try {
                            isFull.await();
                        } catch (InterruptedException e) {
                            LOGGER.info("isFull wait interrupted.", e);
                        }
                    }
                    writeIndex++;
                    if (writeIndex == maxBuffs) writeIndex = 0;
                    currBuffer = msgBuffers.get(writeIndex);
                    currBuffer.clear();
                }
                currBuffer.add(msg);
            } finally {
                isEmpty.signalAll();
                lock.unlock();
            }
        }
        
        /**
         * Gets the next available message batch as a message buffer and
         * advances the current read position to the next available buffer.
         * Buffers returned by this method become "owned" by the caller. When
         * the caller is done with them, {@link #returnBuffer(List)} should
         * be called to return them to the multi-buffer for reuse.
         */
        public List<AMPSSourceRecord> rotateBuffer() {
            lock.lock();
            try {
                LOGGER.debug("Entering rotateBuffer(). ---------------------------------------------------------------------------");
                LOGGER.debug("readIndex: {}", readIndex);
                LOGGER.debug("writeIndex: {}", writeIndex);
                LOGGER.debug("rolledback: {}", rolledback);
                if (rolledback && rollbackBuffer.size() > 0) {
                    return rollbackBuffer;
                }

                // Kafka Connect source task poll() is supposed to block for
                // awhile before returning  if there are no messages to read.
                if (msgBuffers.get(readIndex).size() == 0) {
                    try {
                        isEmpty.await(1000L, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.debug("isEmpty wait interrupted.", e);
                    }
                }

                if (readIndex == writeIndex) {
                    currBuffer = getBuffer();
                    return msgBuffers.set(readIndex, currBuffer);
                }

                int oldReadIndex = readIndex++;
                if (readIndex == maxBuffs) readIndex = 0;

                return msgBuffers.set(oldReadIndex, getBuffer());
            } finally {
                try {
                    rolledback = false;
                    isFull.signalAll();
                    LOGGER.debug("Leaving rotateBuffer(). -----------------------------------------------------------------------");
                } finally {
                    lock.unlock();
                }
            }
        }
        
        /**
         * When an error occurs, this allows
         * the source to push the failed batch back into the multi-buffer so
         * that it will be re-delivered upon the next call
         * {@link #rotateBuffer()}, thus allowing it to be retried.
         */
        public void pushBack(List<AMPSSourceRecord> list) {
            lock.lock();
            try {
                if (list != rollbackBuffer) {
                    rollbackBuffer.clear();
                    rollbackBuffer.addAll(list);
                }
                rolledback = true;
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Allows a message buffer returned by {@link #rotateBuffer()} to be
         * returned to the multi-buffer for reuse. After calling this method
         * the caller should no longer perform any operations on the passed in
         * list.
         */
        public void returnBuffer(List<AMPSSourceRecord> list) {
            lock.lock();
            try {
                list.clear();
                stack.push(list);
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Internal helper method that provides an empty message buffer for
         * reuse, or if one is not available, it constructs a new one.
         * 
         * NOTE: This method requires external synchronization on the
         * multi-buffer's monitor.
         */
        protected List<AMPSSourceRecord> getBuffer() {
            if (!stack.isEmpty())
                return stack.pop();
            
            return new LinkedList<AMPSSourceRecord>();
        }
    }
}
