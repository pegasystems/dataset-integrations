package com.pega.hdfs;/*
 *
 * Copyright (c) 2023 Pegasystems Inc.
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

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public final class ClientConfiguration {

    private static final String CLIENT_PROPERTIES_PREFIX = "client.";

    private final Configuration configuration = new Configuration();
    private final String clientPrincipal;
    private final String keytabPath;
    private final String testWorkDir;
    private final String krb5ConfPath;

    public ClientConfiguration(File file) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            properties.load(fis);
        }
        properties.keySet().stream().map(Object::toString).filter(k -> !isClientProperty(k)).forEach(k -> configuration.set(k, properties.getProperty(k)));
        clientPrincipal = properties.getProperty(clientPropertyFullName("principal"));
        keytabPath = properties.getProperty(clientPropertyFullName("keytabPath"));
        testWorkDir = properties.getProperty(clientPropertyFullName("testWorkDir"));
        if (testWorkDir == null) {
            throw new IllegalArgumentException(clientPropertyFullName("testWorkDir") + " property is required");
        }
        krb5ConfPath = properties.getProperty(clientPropertyFullName("krb5conf"));
    }

    public Configuration hadoopConfiguration() {
        return configuration;
    }

    public Optional<String> clientPrincipal() {
        return Optional.ofNullable(clientPrincipal);
    }

    public Optional<String> keytabPath() {
        return Optional.ofNullable(keytabPath);
    }

    public Optional<String> krb5ConfPath() {
        return Optional.ofNullable(krb5ConfPath);
    }

    public String testWorkDir() {
        return testWorkDir;
    }

    private static boolean isClientProperty(String key) {
        return key.startsWith(CLIENT_PROPERTIES_PREFIX);
    }

    private static String clientPropertyFullName(String name) {
        return CLIENT_PROPERTIES_PREFIX + name;
    }
}
