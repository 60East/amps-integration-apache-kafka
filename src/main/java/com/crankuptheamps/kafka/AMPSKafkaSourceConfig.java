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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import static com.crankuptheamps.kafka.Constants.*;

/**
 * A simple class that creates the configuration definition for the source connector.
 */
public class AMPSKafkaSourceConfig extends AbstractConfig{
    public static final ConfigDef CONFIG_DEF = createConfigDef();

    public AMPSKafkaSourceConfig(Map<?,?> properties) {
        super(CONFIG_DEF, properties);
    }

    
    protected static ConfigDef createConfigDef() {
        ConfigDef config = new ConfigDef();

        config.define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.HIGH, TOPIC_DOC);
        config.define(URI_CONFIG, Type.STRING, URI_DEFAULT, Importance.HIGH, URI_DOC);
        config.define(CLIENT_FACTORY_CONFIG, Type.STRING, CLIENT_FACTORY_DEFAULT, Importance.LOW, CLIENT_FACTORY_DOC);
        config.define(CLIENT_NAME_CONFIG, Type.STRING, CLIENT_NAME_DEFAULT, Importance.HIGH, CLIENT_NAME_DOC);
        config.define(TASK_FILTER_CONFIG, Type.STRING, TASK_FILTER_DEFAULT, Importance.MEDIUM, TASK_FILTER_DOC);
        config.define(FILTER_CONFIG, Type.STRING, FILTER_DEFAULT, Importance.MEDIUM, FILTER_DOC);
        config.define(OPTIONS_CONFIG, Type.STRING, OPTIONS_DEFAULT, Importance.MEDIUM, OPTIONS_DOC);
        config.define(EVENT_HEADERS_CONFIG, Type.STRING, EVENT_HEADERS_DEFAULT, Importance.MEDIUM, EVENT_HEADERS_DOC);
        config.define(CMD_TYPE_FILTER_CONFIG, Type.STRING, CMD_TYPE_FILTER_DEFAULT, Importance.MEDIUM, CMD_TYPE_FILTER_DOC);
        config.define(MAX_BATCH_CONFIG, Type.INT, MAX_BATCH_DEFAULT, Importance.MEDIUM, MAX_BATCH_DOC);
        config.define(MAX_BUFFS_CONFIG, Type.INT, MAX_BUFFS_DEFAULT, Importance.MEDIUM, MAX_BUFFS_DOC);
        config.define(COMMAND_CONFIG, Type.STRING, COMMAND_DEFAULT, Importance.MEDIUM, COMMAND_DOC);
        config.define(KAFKA_KEY_CONFIG, Type.STRING, KAFKA_KEY_DEFAULT, Importance.MEDIUM, KAFKA_KEY_DOC);

        config.define(QUEUE_CONFIG, Type.STRING, QUEUE_DEFAULT, Importance.MEDIUM, QUEUE_DOC);
        config.define(MAX_BACKLOG_CONFIG, Type.STRING, MAX_BACKLOG_DEFAULT, Importance.MEDIUM, MAX_BACKLOG_DOC);
        config.define(ACK_BATCH_CONFIG, Type.INT, ACK_BATCH_DEFAULT, Importance.MEDIUM, ACK_BATCH_DOC);
        config.define(ACK_TIMEOUT_CONFIG, Type.INT, ACK_TIMEOUT_DEFAULT, Importance.MEDIUM, ACK_TIMEOUT_DOC);

        config.define(BOOKMARK_CONFIG, Type.STRING, BOOKMARK_DEFAULT, Importance.MEDIUM, BOOKMARK_DOC);
        config.define(BOOKMARK_STORE_CONFIG, Type.STRING, BOOKMARK_STORE_DEFAULT, Importance.MEDIUM, BOOKMARK_STORE_DOC);
        config.define(BOOKMARK_LOG_CONFIG, Type.STRING, BOOKMARK_LOG_DEFAULT, Importance.MEDIUM, BOOKMARK_LOG_DOC);
        config.define(SUB_ID_CONFIG, Type.STRING, SUB_ID_DEFAULT, Importance.MEDIUM, SUB_ID_DOC);
        config.define(PRUNE_TIME_THRESHOLD_CONFIG, Type.LONG, PRUNE_TIME_THRESHOLD_DEFAULT, Importance.MEDIUM, PRUNE_TIME_THRESHOLD_DOC);

        return config;
    }
}
