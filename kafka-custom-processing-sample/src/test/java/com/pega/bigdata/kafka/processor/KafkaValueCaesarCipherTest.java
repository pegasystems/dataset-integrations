package com.pega.bigdata.kafka.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;
import org.junit.jupiter.api.Test;

import static com.pega.bigdata.kafka.processor.MessageTransformator.ENCRYPTION_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaValueCaesarCipherTest {
    private static final String MESSAGE = "abcd";
    private static final int SHIFT = 3;
    private final KafkaValueCaesarCipherEncryptor kafkaValueCaesarCipherEncryptor = new KafkaValueCaesarCipherEncryptor();
    private final KafkaValueCaesarCipherDecryptor kafkaValueCaesarCipherDecryptor = new KafkaValueCaesarCipherDecryptor();

    @Test
    public void shouldCypherAndDecipherMessage() {
        ClipboardPage headersPage = mock(ClipboardPage.class);
        when(headersPage.getString(ENCRYPTION_KEY)).thenReturn(Integer.toString(SHIFT));
        assertArrayEquals(
                MESSAGE.getBytes(),
                kafkaValueCaesarCipherDecryptor.apply(
                        kafkaValueCaesarCipherEncryptor.apply(MESSAGE.getBytes(), headersPage),
                        headersPage
                )
        );
    }

}
