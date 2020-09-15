package com.pega.bigdata.kafka.processor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.pega.bigdata.kafka.processor.ProcessorTest.checkProcessingCorrectness;

public class KafkaValueCaesarCipherEncryptorTest {
    private static final int SHIFT = 3;
    private static final int BYTE_RANGE = 256;
    private static final String MESSAGE_TO_ENCRYPT = "abcd";
    private static final String EXPECTED_ENCRYPTED_MESSAGE = "defg";
    private final KafkaValueCaesarCipherEncryptor kafkaValueCaesarCipherEncryptor = new KafkaValueCaesarCipherEncryptor();

    @ParameterizedTest
    @ValueSource(ints = {SHIFT, (BYTE_RANGE + SHIFT), (SHIFT - BYTE_RANGE)})
    public void shouldIncreaseMessageBySpecifiedValue(int shift) {
        checkProcessingCorrectness(shift, kafkaValueCaesarCipherEncryptor, EXPECTED_ENCRYPTED_MESSAGE, MESSAGE_TO_ENCRYPT);
    }

}
