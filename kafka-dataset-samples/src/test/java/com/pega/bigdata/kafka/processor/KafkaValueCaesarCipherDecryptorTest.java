package com.pega.bigdata.kafka.processor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.pega.bigdata.kafka.processor.ProcessorTest.checkProcessingCorrectness;

public class KafkaValueCaesarCipherDecryptorTest {
    private static final int SHIFT = 3;
    private static final int BYTE_RANGE = 256;
    private static final String MESSAGE_TO_DECRYPT = "def";
    private static final String EXPECTED_DECRYPTED_MESSAGE = "abc";
    private final KafkaValueCaesarCipherDecryptor kafkaValueCaesarCipherDecryptor = new KafkaValueCaesarCipherDecryptor();

    @ParameterizedTest
    @ValueSource(ints = {SHIFT, (BYTE_RANGE + SHIFT), (SHIFT - BYTE_RANGE)})
    public void shouldDecreaseMessageBySpecifiedValue(int shift) {
        checkProcessingCorrectness(shift, kafkaValueCaesarCipherDecryptor, EXPECTED_DECRYPTED_MESSAGE, MESSAGE_TO_DECRYPT);
    }

}
