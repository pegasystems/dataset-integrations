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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

class MessageSender {

    private static Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private final Producer<byte[], byte[]> producer;
    private final String topicName;

    MessageSender(Producer<byte[], byte[]> producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    void sendMessage(String message) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, message.getBytes(StandardCharsets.UTF_8));
        String messageStart = message.substring(0, 10);
        logger.debug("Sending message '{}'", messageStart);
        long nanoStart = System.nanoTime();
        Future<RecordMetadata> future = producer.send(record);
        logger.debug("Message '{}' is queued", messageStart);
        try {
            future.get();
            long nanoSpent = System.nanoTime() - nanoStart;
            logger.debug("Ack for message '{}' received", messageStart);
            int secSpent = (int) (nanoSpent / 1_000_000_000);
            if (secSpent > 1) {
                logger.error("Slow request for message '{}' : {} sec", messageStart, secSpent);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for ack for message '{}'", messageStart);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Failed to send message " + messageStart, e);
        }
    }
}
