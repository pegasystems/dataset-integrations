package com.pega.bigdata.kafka.keys;

import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.decision.dsm.strategy.clipboard.DSMPropertyInfoProvider;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.dictionary.ImmutablePropertyInfo;
import com.pega.pegarules.pub.util.HashStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageKeyDummySerdeTest {

    @Test
    public void testSampleKeysSerialization() {
        MessageKeyDummySerde messageKeyDummySerde = new MessageKeyDummySerde();

        Map<String, List> config = new HashMap<>();
        config.put("keys", Collections.singletonList("DummyKeyId"));

        messageKeyDummySerde.configure(null, config);

        ClipboardPage clipboardPage = mock(ClipboardPage.class);
        when(clipboardPage.getString("DummyKeyId")).thenReturn("1");

        byte[] actual = messageKeyDummySerde.serialize(null, clipboardPage);
        assertEquals("1:dummy".getBytes(StandardCharsets.UTF_8), actual);
    }

}