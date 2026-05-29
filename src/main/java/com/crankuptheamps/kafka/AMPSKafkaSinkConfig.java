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
 * A simple class that creates the configuration definition for the sink connector.
 */
public class AMPSKafkaSinkConfig extends AbstractConfig{
    public static final ConfigDef CONFIG_DEF = createConfigDef();

    public AMPSKafkaSinkConfig(Map<?,?> properties) {
        super(CONFIG_DEF, properties);
    }

    protected static ConfigDef createConfigDef() {
        ConfigDef config = new ConfigDef();

        config.define(TOPIC_CONFIG, Type.STRING, TOPIC_DEFAULT, Importance.HIGH, TOPIC_DOC);
        config.define(URI_CONFIG, Type.STRING, URI_DEFAULT, Importance.HIGH, URI_DOC);
        config.define(CLIENT_FACTORY_CONFIG, Type.STRING, CLIENT_FACTORY_DEFAULT, Importance.LOW, CLIENT_FACTORY_DOC);
        config.define(CLIENT_NAME_CONFIG, Type.STRING, CLIENT_NAME_DEFAULT, Importance.HIGH, CLIENT_NAME_DOC);
        config.define(MAX_BATCH_CONFIG, Type.INT, MAX_BATCH_DEFAULT, Importance.MEDIUM, MAX_BATCH_DOC);
        config.define(USE_TOPIC_HEADER_CONFIG, Type.BOOLEAN, USE_TOPIC_HEADER_DEFAULT, Importance.MEDIUM, USE_TOPIC_HEADER_DOC);
        config.define(PUBLISH_FLUSH_TIMEOUT_CONFIG, Type.LONG, PUBLISH_FLUSH_TIMEOUT_DEFAULT, Importance.MEDIUM, PUBLISH_FLUSH_TIMEOUT_DOC);
        config.define(LOG_MSG_ON_WRITE_ERROR_CONFIG, Type.BOOLEAN, LOG_MSG_ON_WRITE_ERROR_DEFAULT, Importance.MEDIUM, LOG_MSG_ON_WRITE_ERROR_DOC);
        
        config.define(PUB_STORE_TYPE_CONFIG, Type.STRING, PUB_STORE_TYPE_DEFAULT, Importance.MEDIUM, PUB_STORE_TYPE_DOC);
        config.define(PUB_STORE_INIT_CAP_CONFIG, Type.INT, PUB_STORE_INIT_CAP_DEFAULT, Importance.MEDIUM, PUB_STORE_INIT_CAP_DOC);
        config.define(PUB_STORE_PATH_CONFIG, Type.STRING, PUB_STORE_PATH_DEFAULT, Importance.MEDIUM, PUB_STORE_PATH_DOC);
        config.define(PRUNE_TIME_THRESHOLD_CONFIG, Type.LONG, PRUNE_TIME_THRESHOLD_DEFAULT, Importance.MEDIUM, PRUNE_TIME_THRESHOLD_DOC);

        return config;
    }
}
