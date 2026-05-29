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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector; 

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.crankuptheamps.kafka.AMPSKafkaSourceConfig.*;
import static com.crankuptheamps.kafka.Constants.*;

/**
 * A Kafka source connector that passes messages from an AMPS server to Kafka connect.
 */
public class AMPSKafkaSource extends SourceConnector {

    /**
     * The source connector configuration.
     */
    private Map<String, String> properties;

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AMPSKafkaSource.class);

    /**
     * @return configuration defintion.
     */
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    
    /**
     * @return version of connector.
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Defines the class that should be instantiated in worker processes to read the data.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return AMPSKafkaSourceTask.class;
    }

    /**
     * Generates configuration sets for individual tasks based on the requested maximum.
     * produces a unique configuration for each task, assigning distinct 
     * client names and applying a partitioning filter when multiple tasks are requested.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // User must provide filter expression if max.tasks > 1
        String filterFormat = properties.get(TASK_FILTER_CONFIG);
        if (maxTasks > 1) {
            if (filterFormat == null || filterFormat.isEmpty()) {
                throw new ConfigException(TASK_FILTER_CONFIG + " is required when max.tasks is greater than 1. ");
            }
        }

		Map<String, String> temp = new HashMap<>(properties);
        String name = "";
        String taskFilter = "";
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        String baseFilter = properties.get(FILTER_CONFIG);

        // Create unique client name
        for(int i = 0; i < maxTasks; i++) {
            name = properties.get(CLIENT_NAME_CONFIG) + "_" + i;
            temp.put(CLIENT_NAME_CONFIG, name);

            // Create one task per task set and apply the task-specific filter
            if (maxTasks > 1) {
                // Generate the task-specific filter using the configurable format string
                taskFilter = String.format(filterFormat, maxTasks, i);

                // If a base filter already exists, safely append the new filter
                if (baseFilter != null && !baseFilter.isEmpty()) {
                    temp.put(FILTER_CONFIG, "(" + baseFilter + ") AND (" + taskFilter + ")");
                } else {
                    temp.put(FILTER_CONFIG, taskFilter);
                }
            }

            taskConfigs.add(new HashMap<>(temp));
        }

        return taskConfigs;
    }

    /**
     * Validates the connector's configuration.
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        LOGGER.info("Validating ---------------------------------------------------");
        Config config = super.validate(connectorConfigs);
        return config;
    }

    /**
     * Starts the source connector for the Kafka worker.
     */
    @Override
    public void start(Map<String, String> properties) {
        LOGGER.debug("Starting AMPSKafkaSource ------------------------------------------------------------------");
        this.properties = properties;

    }

    /**
     * Stops the source connector for the Kafka worker.
     */
    @Override
    public void stop() {
        LOGGER.debug("Stopping AMPSKafkaSource ------------------------------------------------------------------");
    }
}