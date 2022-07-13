/*
 *
 * Copyright (c) 2022  Pegasystems Inc.
 * All rights reserved.
 *
 * This  software  has  been  provided pursuant  to  a  License
 * Agreement  containing  restrictions on  its  use.   The  software
 * contains  valuable  trade secrets and proprietary information  of
 * Pegasystems Inc and is protected by  federal   copyright law.  It
 * may  not be copied,  modified,  translated or distributed in  any
 * form or medium,  disclosed to third parties or used in any manner
 * not provided for in  said  License Agreement except with  written
 * authorization from Pegasystems Inc.
 */
package com.pega.test.kafka;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

//not thread safe
class MessageGenerator implements Iterator<String> {
    private static Logger logger = LoggerFactory.getLogger(MessageGenerator.class);
    private final int messageSize;
    private final int messagesPerMinute;
    private final int count;
    private int generatedCount;
    private long firstMessageTime;

    MessageGenerator(int messageSize, int messagesPerMinute, int count) {
        this.messageSize = messageSize;
        this.messagesPerMinute = messagesPerMinute;
        this.count = count;
    }

    @Override
    public boolean hasNext() {
        return generatedCount < count;
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        controlThroughput();
        generatedCount++;
        return Message.ofSize(messageSize);
    }

    private void controlThroughput() {
        long timeToWait = timeToWaitMs();
        logger.debug("Time to sleep ms: {}", timeToWait);
        if (timeToWait > 0) {
            try {
                Thread.sleep(timeToWait);
                logger.debug("Slept ms: {}", timeToWait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private long timeToWaitMs() {
        if (firstMessageTime == 0) {
            firstMessageTime = System.currentTimeMillis();
            return 0;
        }
        long timeElapsed = System.currentTimeMillis() - firstMessageTime;
        return 60000 * generatedCount / messagesPerMinute - timeElapsed;
    }
}
