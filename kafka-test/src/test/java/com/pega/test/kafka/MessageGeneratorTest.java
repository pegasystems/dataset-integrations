package com.pega.test.kafka;
/*
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageGeneratorTest {

    @Test
    public void shouldGenerateCorrectSize() {
        MessageGenerator generator = new MessageGenerator(123, 60 * 1000 * 1000, 1);
        assertThat(generator.next()).hasSize(123);
    }

    @Test
    public void shouldGenerateCorrectNumberOfMessages() {
        MessageGenerator generator = new MessageGenerator(123, 60 * 1000 * 1000, 12);
        assertThat(elements(generator)).hasSize(12);
    }

    @Test
    public void shouldControlThroughputWhenFrequencyMoreThanOneMessagePerMs() {
        MessageGenerator generator = new MessageGenerator(123, 60 * 1000 * 5, 5000);
        long start = System.nanoTime();
        assertThat(elements(generator)).hasSize(5000);
        long elapsedTime = System.nanoTime() - start;
        assertThat(elapsedTime * 0.8).isLessThan(TimeUnit.SECONDS.toNanos(1));
        assertThat(elapsedTime * 1.2).isGreaterThan(TimeUnit.SECONDS.toNanos(1));
    }

    private List<String> elements(Iterator<String> it) {
        List<String> list = new ArrayList<>();
        while (it.hasNext()) {
            list.add(it.next());
        }
        return list;
    }
}
