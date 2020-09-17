/*
 *
 * Copyright (c) 2019 Pegasystems Inc.
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

package com.pega.dsm.kafka.api.serde.serializer;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.platform.kafka.serde.PegaSerde;

import java.util.List;
import java.util.Map;

public class MessageKeyDummySerde implements PegaSerde {
    private Map<String, ?> config;

    @Override
    public void configure(PublicAPI tools, Map<String, ?> configs) {
        this.config = configs;
    }

    @Override
    public byte[] serialize(PublicAPI tools, ClipboardPage clipboardPage) {
        List<String> keys = (List<String>) config.get("keys");
        ByteArrayDataOutput bos = ByteStreams.newDataOutput();
        for (String key : keys) {
            if (key.startsWith("DummyKey")) {
                String value = clipboardPage.getString(key) + ":dummy";
                bos.write(
                    value.getBytes(com.google.common.base.Charsets.UTF_8)
                );
            }

        }
        return bos.toByteArray();
    }

    @Override
    public ClipboardPage deserialize(PublicAPI tools, byte[] data) {
        //not needed
        return null;
    }
}
