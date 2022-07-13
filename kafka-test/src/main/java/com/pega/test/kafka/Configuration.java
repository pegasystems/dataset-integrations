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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

class Configuration {

    private static Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static final String PRODUCER_PROPERTY_PREFIX = "producer.";
    private static final String TOPIC_NAME_PROPERTY = "topic.name";
    private static final String THREADS_PROPERTY = "threads";
    private static final String MESSAGE_SIZE_PROPERTY = "message.size";
    private static final String THROUGHPUT_PROPERTY = "throughput";
    private static final String MESSAGE_COUNT_PROPERTY = "message.count";

    private final Properties producerProperties;
    private final String topicName;
    private final int threads;
    private final int messageSize;
    private final int throughput;
    private final int messageCount;

    Configuration(String propertiesFileName) {
        Properties properties = propertiesFromFile(propertiesFileName);
        logger.debug("Client properties >>>: {}", properties);
        producerProperties = new Properties();
        for (String propertyName : properties.stringPropertyNames()) {
            if (propertyName.startsWith(PRODUCER_PROPERTY_PREFIX)) {
                producerProperties.put(propertyName.substring(PRODUCER_PROPERTY_PREFIX.length()), properties.getProperty(propertyName));
            }
        }
        topicName = properties.getProperty(TOPIC_NAME_PROPERTY);
        threads = Integer.parseInt(properties.getProperty(THREADS_PROPERTY, "1"));
        messageSize = Integer.parseInt(properties.getProperty(MESSAGE_SIZE_PROPERTY, "1000"));
        throughput = Integer.parseInt(properties.getProperty(THROUGHPUT_PROPERTY, "10"));
        messageCount = Integer.parseInt(properties.getProperty(MESSAGE_COUNT_PROPERTY, "10"));
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getThreads() {
        return threads;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public int getThroughput() {
        return throughput;
    }

    public int getMessageCount() {
        return messageCount;
    }

    private Properties propertiesFromFile(String propertiesFileName) {
        try (InputStream fileStream = new FileInputStream(propertiesFileName)) {
            Properties properties = new Properties();
            properties.load(fileStream);
            return properties;
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
