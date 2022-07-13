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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Properties file path must be provided");
            return;
        }
        String fileName = args[0];

        Configuration configuration = new Configuration(fileName);
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(configuration.getProducerProperties())) {
            if (configuration.getThreads() > 1) {
                concurrentExecution(configuration, producer);
            } else {
                singleThreadExecution(configuration, producer);
            }
        }
    }

    private static void singleThreadExecution(Configuration configuration, Producer<byte[], byte[]> producer) {
        MessageSender sender = new MessageSender(producer, configuration.getTopicName());
        MessageGenerator generator = new MessageGenerator(configuration.getMessageSize(),
                60 * configuration.getThroughput(),
                configuration.getMessageCount());
        while (generator.hasNext()) {
            sender.sendMessage(generator.next());
        }
    }

    private static void concurrentExecution(Configuration configuration, Producer<byte[], byte[]> producer) {
        ExecutorService service = Executors.newFixedThreadPool(configuration.getThreads());
        for (int i = 0; i < configuration.getThreads(); i++) {
            service.submit(() -> {
                MessageSender sender = new MessageSender(producer, configuration.getTopicName());
                MessageGenerator generator = new MessageGenerator(configuration.getMessageSize(),
                        60 * configuration.getThroughput() / configuration.getThreads(),
                        configuration.getMessageCount() / configuration.getThreads());
                while (generator.hasNext()) {
                    sender.sendMessage(generator.next());
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(configuration.getMessageCount() / configuration.getThroughput(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
