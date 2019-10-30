package com.pega.integration.kafka.testutils;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestUtils {

    public static String getFileContentAsString(String resourceName) throws Exception {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName)) {
            return IOUtils.toString(in, UTF_8);
        }
    }
}
