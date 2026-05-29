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

import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.MemoryBookmarkStore;
import com.crankuptheamps.client.MemoryPublishStore;
import com.crankuptheamps.client.PublishStore;
import com.crankuptheamps.client.util.SerializableFunction;

import static com.crankuptheamps.kafka.Constants.*;

import java.beans.ExceptionListener;
import java.util.Properties;

/**
 * Simple implementation of the {@link SerializableFunction<Properties, Client>}
 * client factory interface from the AMPS Client JAR. This is implemented
 * to return an AMPS HAClient based upon parameters in the Kafka config Properties.
 * More advanced implementations could setup an HAClient with custom server chooser 
 * and authenticator implementations. 
 */
public class AMPSBasicClientFunction
        implements SerializableFunction<AbstractConfig, Client> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AMPSBasicClientFunction.class);

    /**
     * Executes this function using the passed in configuration
     * {@link Properties}, to construct and return an AMPS client for use by
     * the Kafka source.
     * 
     * @param config The Properties object that holds the client's
     *        configuration as a set of key/value pairs.
     * 
     * @return The newly created AMPS client.
     */
    @Override
    public Client apply(AbstractConfig config) {
        DefaultServerChooser svrChooser = new DefaultServerChooser();
        Client client = null;
        
        try {
            // Calculate client name.
            String clientName = config.getString(CLIENT_NAME_CONFIG);
            
            // Get server URI(s) and setup server chooser.
            String uriString = config.getString(URI_CONFIG);
            String[] uris = uriString.split(",");
            for (String serverURI : uris) {
                serverURI = serverURI.trim();
                if (serverURI != null && !serverURI.isEmpty()) {
                    svrChooser.add(serverURI);
                }
            }

            // Create client.
            client = new HAClient(clientName);
             ((HAClient) client).setServerChooser(svrChooser);

            // Add logging exception listener to client.
            client.setExceptionListener(new ExceptionListener() {
                @Override
                public void exceptionThrown(Exception e) {
                    LOGGER.error("AMPS client received error: " + e, e);
                }
            });

            // If source client, setup appropriate configs.
            if (config.values().containsKey(BOOKMARK_CONFIG)){
                // Setup bookmark store if needed.
                String bookmark = config.getString(BOOKMARK_CONFIG);
                String bStoreType = config.getString(BOOKMARK_STORE_CONFIG);
                String subIdStr = config.getString(SUB_ID_CONFIG);
                if (bookmark != null && !bookmark.isEmpty()) {
                    if (bStoreType != null && !bStoreType.isEmpty()) {
                        if (subIdStr == null || subIdStr.isEmpty()) {
                            throw new IllegalArgumentException(SUB_ID_CONFIG +
                                     " must be set when using a bookmark subscription.");
                        }
                        if ("memory".equalsIgnoreCase(bStoreType)) {
                            client.setBookmarkStore(new MemoryBookmarkStore());
                        } else if ("logged".equalsIgnoreCase(bStoreType)) {
                            String storePath = config.getString(BOOKMARK_LOG_CONFIG);
                            if (storePath == null || storePath.isEmpty()) {
                                throw new IllegalArgumentException(BOOKMARK_LOG_CONFIG +
                                    " must be specified for a logged bookmark"
                                    + " store.");
                            }
                            client.setBookmarkStore(new LoggedBookmarkStore(storePath));
                        }
                    }
                }
                // Setup message acking if needed.
                String queue = config.getString(QUEUE_CONFIG);
                if ("y".equalsIgnoreCase(queue)) {
                    int ackBatch = config.getInt(ACK_BATCH_CONFIG);
                    int ackTimeout = config.getInt(ACK_TIMEOUT_CONFIG);
                    client.setAckBatchSize(ackBatch);
                    client.setAckTimeout(ackTimeout);
                }
            }

            // If sink client, setup appropriate configs.
            if (config.values().containsKey(PUB_STORE_TYPE_CONFIG)){
                // Setup publish store if needed.
                String pubStoreType = config.getString(PUB_STORE_TYPE_CONFIG);
                if (pubStoreType != null && !pubStoreType.isEmpty()) {
                    int initialCapacity = config.getInt(PUB_STORE_INIT_CAP_CONFIG);
                    if ("memory".equalsIgnoreCase(pubStoreType)) {
                        client.setPublishStore(new MemoryPublishStore(initialCapacity));
                    } else if ("file".equalsIgnoreCase(pubStoreType)) {
                        String path = config.getString(PUB_STORE_PATH_CONFIG);
                        if (path == null || path.isEmpty()) {
                            throw new IllegalArgumentException(PUB_STORE_PATH_CONFIG +
                                    " must be specified for a file-backed publish"
                                    + " store.");
                        }
                        client.setPublishStore(new PublishStore(path, initialCapacity));
                    }
                }
            }

            return client;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create AMPS client: " + e, e);
        }
    }
}
