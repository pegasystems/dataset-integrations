package com.pega.bigdata.kafka.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;

import java.util.function.BiFunction;

import static com.pega.bigdata.kafka.processor.MessageTransformator.getShift;
import static com.pega.bigdata.kafka.processor.MessageTransformator.transform;

/**
 * Sample of custom post-processing class, which is performing opposite operation to KafkaValueCaesarCipherEncryptor.
 * It adds number specified in header with opposite sign to each byte of message read from Kafka topic.
 *
 * @see KafkaValueCaesarCipherEncryptor to check full description of pre/post processors implementation details.
 */
public class KafkaValueCaesarCipherDecryptor implements BiFunction<byte[], ClipboardPage, byte[]> {

    @Override
    public byte[] apply(byte[] message, ClipboardPage headers) {
        return transform(message, changeShiftSign(getShift(headers)));
    }

    private int changeShiftSign(int shift) {
        return (-1) * shift;
    }

}