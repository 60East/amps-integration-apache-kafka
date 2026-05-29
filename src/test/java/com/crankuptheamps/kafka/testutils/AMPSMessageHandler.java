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

package com.crankuptheamps.kafka.testutils;

import java.util.concurrent.LinkedBlockingQueue;

import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;

public class AMPSMessageHandler implements MessageHandler {
    public final LinkedBlockingQueue<ReceivedMessage> messages;

    public AMPSMessageHandler(int capacity) {
        messages = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public void invoke(Message message) {
        try {
            messages.put(new ReceivedMessage(message.getTopic(), message.getData()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class ReceivedMessage {
        public final String topic;
        public final String data;

        public ReceivedMessage(String topic, String data) {
            this.topic = topic;
            this.data = data;
        }
    }
}
