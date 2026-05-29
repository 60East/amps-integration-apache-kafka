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

import org.apache.kafka.common.config.ConfigDef;

import com.crankuptheamps.client.Message;

/**
 * Configuration key constants used to pull configuration values from the
 * Kafka context during start-up.
 */
public interface Constants { 
    /**
     * The name of the class to be used to construct the AMPS client from
     * the configuration. If not specified, then
     * com.crankuptheamps.kafka.AMPSBasicClientFunction will be used.
     */
    String CLIENT_FACTORY_CONFIG = "clientFactoryClass";
    String CLIENT_FACTORY_DEFAULT = "com.crankuptheamps.kafka.AMPSBasicClientFunction";
    String CLIENT_FACTORY_DOC = "The name of the class to be used to construct the AMPS" 
        + "client from the configuration.";
    
    /**
     * Configuration key used to get the AMPS client name from the
     * Kafka context.
     */
    String CLIENT_NAME_CONFIG = "clientName";
    Object CLIENT_NAME_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;
    String CLIENT_NAME_DOC = "The name of the AMPS client.";
    
    /**
     * Configuration key used to get the bookmark type to be used
     * while subscribing. Accepts any valid AMPS bookmark.
     */
    String BOOKMARK_CONFIG = "bookmark";
    String BOOKMARK_DEFAULT = null;
    String BOOKMARK_DOC = "The bookmark type.";

    /**
     * Configuration key used to get the bookmark store type to be used
     * while subscribing. Possible values are "memory" for a memory-backed
     * bookmark store, or "logged" for a file-backed bookmark store.
     */
    String BOOKMARK_STORE_CONFIG = "bookmarkStoreType";
    String BOOKMARK_STORE_DEFAULT = null;
    String BOOKMARK_STORE_DOC = "The bookmark store type.";
    
    /**
     * Configuration key used to get the bookmark log file path/name from the
     * Kafka context.
     */
    String BOOKMARK_LOG_CONFIG = "bookmarkLog";
    String BOOKMARK_LOG_DEFAULT = null;
    String BOOKMARK_LOG_DOC = "The bookmark log file path/name.";

    /**
     * Configuration key used to get the publish store type from the
     * Kafka context. Possible values are "memory" for a memory-backed publish store,
     * or "file" for a file-backed publish store.
     */
    String PUB_STORE_TYPE_CONFIG = "pubStoreType";
    String PUB_STORE_TYPE_DEFAULT = null;
    String PUB_STORE_TYPE_DOC = "The publish store type.";

    /**
     * Configuration key used to get the publish store's initial capacity
     * (in 2KB blocks) from the Kafka context. Default is 1000 blocks.
     */
    String PUB_STORE_INIT_CAP_CONFIG = "pubStoreInitialCap";
    int PUB_STORE_INIT_CAP_DEFAULT = 1000;
    String PUB_STORE_INIT_CAP_DOC = "The publish store's initial capacity.";


    /**
     * Configuration key used to get the publish store's backing file path from
     * the Kafka context. Required if using a file-backed publish store.
     */
    String PUB_STORE_PATH_CONFIG = "pubStorePath";
    String PUB_STORE_PATH_DEFAULT = null;
    String PUB_STORE_PATH_DOC = "The publish store's backing file path.";
    
    /**
     * Configuration key used to get server URI(s) from the Kafka context.
     * When an HAClient with multiple AMPS servers is desired, provide a 
     * comma-separated list of URIs.
     */
    String URI_CONFIG = "uri";
    Object URI_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;
    String URI_DOC = "AMPS server URI(s): scheme://[userinfo@]host:port/protocol/msgtype";
    
    /**
     * Configuration key used to get the AMPS command type string from the
     * Kafka context. This key is optional. If its value isn't specified
     * it defaults to "subscribe".
     */
    String COMMAND_CONFIG = "command";
    String COMMAND_DEFAULT = "subscribe";
    String COMMAND_DOC = "AMPS command type: subscribe or sow_and_subscribe";

    /**
     * Configuration key used to get the AMPS filter expression from the Kafka context
     * used to separate messages to different tasks from the Kafka context. 
     * Required when max.tasks > 1.
     */
    String TASK_FILTER_CONFIG = "taskFilter";
    String TASK_FILTER_DEFAULT = null;
    String TASK_FILTER_DOC = "The filter expression used to seperate messages to different tasks.";

    /**
     * Configuration key used to get the AMPS topic expression from the
     * Kafka context. For a source this may be a topic or regular expression
     * topic. For a sink it must be an actual topic since it will be published
     * to by default (unless an event specifies the topic it should be published
     * to in its topic header).
     */
    String TOPIC_CONFIG = "ampsTopic";
    Object TOPIC_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;
    String TOPIC_DOC = "The topic to subscribe/publish records.";
    
    /**
     * Configuration key used to get the AMPS filter expression from the
     * Kafka context. This key is optional.
     */
    String FILTER_CONFIG = "filter";
    String FILTER_DEFAULT = null;
    String FILTER_DOC = "The filter expression.";
    
    /**
     * Configuration key used to get the AMPS command options from the
     * Kafka context. This key is optional.
     */
    String OPTIONS_CONFIG = "options";
    String OPTIONS_DEFAULT = null;
    String OPTIONS_DOC = "The AMPS command options.";
    
    /**
     * Configuration key used to get the AMPS subscription ID from the
     * Kafka context. Required for bookmark subscriptions.
     */
    String SUB_ID_CONFIG = "subscriptionId";
    String SUB_ID_DEFAULT = null;
    String SUB_ID_DOC = "The AMPS Subscription ID.";
    
    /**
     * Configuration key used to specify a bit mask of AMPS command type
     * integers that will be stored into configured Kafka channels when received
     * by the source's AMPS client. Any message received whose command type is
     * not in this bit mask will be ignored. By default this mask contains
     * {@link Message.Command.Publish}, {@link Message.Command.SOW},
     * {@link Message.Command.DeltaPublish}, and {@link Message.Command.OOF}
     */
    String CMD_TYPE_FILTER_CONFIG = "cmdTypeFilter";
    String CMD_TYPE_FILTER_DEFAULT = "" + (Message.Command.Publish
        | Message.Command.DeltaPublish | Message.Command.SOW | Message.Command.OOF);
    String CMD_TYPE_FILTER_DOC = "A bit mask of AMPS command type integers that will be "
        + "stored into configured Kafka channels when received by the source's AMPS client.";
    
    /**
     * Configuration key used to get the maximum number of message buffers
     * (for the message multi-buffer) from the Kafka context.
     */
    String MAX_BUFFS_CONFIG = "maxBuffers";
    int MAX_BUFFS_DEFAULT = 10;
    String MAX_BUFFS_DOC = "The max number of message buffers.";

    
    /**
     * Configuration key used to get the maximum batch size from the Kafka
     * context. This is the max number of events that will be written to or
     * read from a channel transaction. This shouldn't be larger than
     * your smallest configured transaction capacity for all Kafka
     * channels used by your AMPS sources / sinks.
     */
    String MAX_BATCH_CONFIG = "maxBatch";
    int MAX_BATCH_DEFAULT = 1000;
    String MAX_BATCH_DOC = "The number of records that should be returned to Kafka in each batch.";
    
    /**
     * Configuration key used to get the pruning time threshold from the
     * Kafka context. When the configured AMPS client is using a
     * LoggedBookmarkStore, the AMPS Kafka source will prune it of old entries
     * whenever it is polled and it finds that at least this many milliseconds
     * have elapsed since the last prune operation or startup. The default is
     * 5 minutes (300,000 milliseconds). Pruning will also be done upon source
     * shutdown.
     * 
     * If this parameter is set to zero, both periodic and shutdown
     * pruning will be turned off. This may be useful for testing and debugging,
     * but is NOT recommended for a production deployment.
     */
    String PRUNE_TIME_THRESHOLD_CONFIG = "pruneTimeThreshold";
    Long PRUNE_TIME_THRESHOLD_DEFAULT = 5 * 60 * 1000L;
    String PRUNE_TIME_THRESHOLD_DOC = "The pruning time threshold.";

    /**
     * Configuration key used to get the use-topic-header flag from the
     * Kafka context. If set to true, for any event read from the sink's channel
     * that has a topic header, it will be published to the topic specified in
     * the event's header (rather than the sink's default configured topic).
     * Defaults to true.
     */
    String USE_TOPIC_HEADER_CONFIG = "useTopicHeader";
    Boolean USE_TOPIC_HEADER_DEFAULT = true;
    String USE_TOPIC_HEADER_DOC = "Should we use the topic header? true/false.";

    /**
     * Configuration key used to get the publish flush timeout from the
     * Kafka context. This is the publish flush executed at the end of the
     * batch. Defaults to zero (i.e. wait indefinitely for whole batch to be
     * persisted). If set to -1, no publish flush is executed at the end of the
     * batch. Warning, not using a publish flush prevents detection of any
     * failed writes before committing and will also lead to committing before
     * the whole batch is persisted by AMPS (this might be appropriate if
     * relying on a file-backed publish store to make sure messages are
     * eventually persisted in AMPS).
     */
    String PUBLISH_FLUSH_TIMEOUT_CONFIG = "publishFlushTimeout";
    Long PUBLISH_FLUSH_TIMEOUT_DEFAULT = -1L;
    String PUBLISH_FLUSH_TIMEOUT_DOC = "The publish flush timeout.";

    /**
     * Configuration key used to get the state of the log-message-on-write-error
     * flag from the Kafka context. If this is true, the failed write handler
     * will write the actual message that failed to the Kafka log when a failed
     * write is detected. NOTE: the whole message will only be available to the
     * failed write handler if the AMPS client is using a publish store. 
     */
    String LOG_MSG_ON_WRITE_ERROR_CONFIG = "logMsgOnWriteError";
    Boolean LOG_MSG_ON_WRITE_ERROR_DEFAULT = true;
    String LOG_MSG_ON_WRITE_ERROR_DOC = "Should we write the log message on write error? true/false.";

    /**
     * Configuration key used to get the AMPS queue option. Tells
     * AMPS client if it is subscribing to a queue. Default is null
     * for no queue, "y" is in case the topic is a queue topic.
     */
    String QUEUE_CONFIG = "isQueue";
    String QUEUE_DEFAULT = null;
    String QUEUE_DOC = "Option for subscribing to a queue.";

    /**
     * Configuration key used to get the maximum number of unacknowledged 
     * messages from a queue that AMPS will hold.
     */
    String MAX_BACKLOG_CONFIG = "maxBacklog";
    String MAX_BACKLOG_DEFAULT = "1";
    String MAX_BACKLOG_DOC = "The max number of unackowledged messages.";

    /**
     * Configuration key used to get the number of messages per acknowledgement
     * batch for an AMPS queue topic. Recommended value should be 1/3 of
     * the max backlog size. A value larger than 1/2 of the max backlog
     * size is not recommended.
     */
    String ACK_BATCH_CONFIG = "ackBatchSize";
    int ACK_BATCH_DEFAULT = 3000;
    String ACK_BATCH_DOC = "The number of messages per acknowledgment batch.";

    /**
     * Configuration key used to get the acknowledgement timeout value. Amount
     * of time in seconds that AMPS will wait after a batch to send a new batch
     * of  acknowledgments.
     */
    String ACK_TIMEOUT_CONFIG = "ackTimeout";
    int ACK_TIMEOUT_DEFAULT = 60000;
    String ACK_TIMEOUT_DOC = "The amount of time to wait since last batch before "
            + "sending the next batch.";
    
    /**
     * <p>
     * Configuration key used to get a comma-separated list of headers that
     * should be added to each Kafka event created from an AMPS message.
     * Possible values include:
     * </p>
     * 
     * <ul>
     * 
     * <li>commandheader - The command type of the AMPS message.</li>
     * 
     * <li>topicheader - The topic name of the AMPS message.</li>
     * 
     * <li>sowKey - The SOW key of the AMPS message. Only set on the message
     * for SOW and SOW and Subscribe queries.</li>
     * 
     * <li>ampsTimestamp - The ISO-8601 timestamp of when AMPS processed the
     * message. Only set on the message when the query or subscription specifies
     * the 'timestamp' option.</li>
     * 
     * <li>bookmark - The unique bookmark string of the AMPS message. Only
     * set on the message for bookmark subscriptions.</li>
     * 
     * <li>correlationId - The correlation Id of the AMPS message.</li>
     * 
     * <li>subId - The subscription Id of the AMPS message.</li>
     * 
     * <li>length - The length of the AMPS message body in bytes.</li>
     * 
     * <li>timestamp - The current timestamp of when the AMPS Kafka source
     * received the AMPS message. Its value is the number of milliseconds
     * since the system clock's epoch, represented as a string.</li>
     * 
     * </ul>
     */
    String EVENT_HEADERS_CONFIG = "eventHeaders";
    String EVENT_HEADERS_DEFAULT = null;
    String EVENT_HEADERS_DOC = "A comma separated list of headers.";

    
    /**
     * Event header key for the message's command type.
     */
    String COMMAND_HEADER_CONFIG = "commandHeader";
    String COMMAND_HEADER_DEFAULT = null;
    String COMMAND_HEADER_DOC = "The event header key for the message's command type.";
    
    /**
     * Event header key for the message's topic.
     */
    String TOPIC_HEADER_CONFIG = "topicHeader";
    String TOPIC_HEADER_DEFAULT = null;
    String TOPIC_HEADER_DOC = "The event header key for the message's topic.";
    
    /**
     * Event header key for the message's sow key.
     */
    String SOW_KEY_HEADER_CONFIG = "sowKey";
    String SOW_KEY_HEADER_DEFAULT = null;
    String SOW_KEY_HEADER_DOC = "The event header key for the message's sow key.";
    
    /**
     * Event header key for the message's ISO-8601 timestamp of when it was
     * processed by the AMPS server.
     */
    String AMPS_TIMESTAMP_HEADER_CONFIG = "ampsTimestamp";
    String AMPS_TIMESTAMP_HEADER_DEFAULT = null;
    String AMPS_TIMESTAMP_HEADER_DOC = "The event header key for the message's ISO-8601 timestamp.";
    
    /**
     * Event header key for the message's bookmark.
     */
    String BOOKMARK_HEADER_CONFIG = "bookmarkHeader";
    String BOOKMARK_HEADER_DEFAULT = null;
    String BOOKMARK_HEADER_DOC = "The event header key for the message's bookmark.";
    
    /**
     * Event header key for the message's correlation ID.
     */
    String CORRELATION_ID_HEADER_CONFIG = "correlationId";
    String CORRELATION_ID_HEADER_DEFAULT = null;
    String CORRELATION_ID_HEADER_DOC = "The event header key for the message's correlation ID.";
    
    /**
     * Event header key for the message's subscription ID.
     */
    String SUB_ID_HEADER_CONFIG = "subId";
    String SUB_ID_HEADER_DEFAULT = null;
    String SUB_ID_HEADER_DOC = "The event header key for the message's subscription ID.";
    
    /**
     * Event header key for the message's body length in bytes.
     */
    String LENGTH_HEADER_CONFIG = "length";
    String LENGTH_HEADER_DEFAULT = null;
    String LENGTH_HEADER_DOC = "The event header key for the message's body length in bytes.";
    
    /**
     * Event header key for the message's current timestamp when it was received
     * by the AMPS Kafka Source.
     */
    String TIMESTAMP_HEADER_CONFIG = "timestamp";
    String TIMESTAMP_HEADER_DEFAULT = null;
    String TIMESTAMP_HEADER_DOC = "The event header for the message's current timestamp.";

    /**
     * In order to maintain strict ordering, it is required to provide
     * a record key for publishing source records into a single Kafka partition.
     */
    String KAFKA_KEY_CONFIG = "key";
    String KAFKA_KEY_DEFAULT = null;
    String KAFKA_KEY_DOC = "The Kafka record key to publish to the same partition and maintain ordering.";

}
