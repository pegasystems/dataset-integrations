package com.pega.bigdata.kafka.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;

import java.util.function.BiFunction;

import static com.pega.bigdata.kafka.processor.MessageTransformator.getShift;
import static com.pega.bigdata.kafka.processor.MessageTransformator.transform;

/**
 * Sample of custom pre-processing class, which can be used to transform Kafka record values.
 * This particular implementation of pre-processing encrypts message with usage of Caesar cipher - it adds specific number to each byte of message.
 * Information about number which should be added to each byte of message should be passed in records header as key-value pair, where key is "encryptionKey" and value is chosen integer.
 * Every class which represents custom processing (pre/post processing) implementation of Kafka record values have to implement BiFunction<byte[], Headers, byte[]>.
 * Parameters of BiFunction interface are:
 * - 1st input argument type - byte[] - input message in form of byte array. This is message form after serialization.
 *   In Kafka DataSet this serialization is implemented, because Kafka topic expects messages in form of byte arrays.
 *   For post-processing 1st argument is message read from topic (message after serialization and pre-processing).
 * - 2nd input argument type - Headers - interface from Kafka common library. Information required to apply pre/post processing can be passed with usage of Headers.
 * - output type - byte[] - output of applying BiFunction on given message is byte array
 *   (message after serialization and pre-processing, or message read from topic after post-processing, ready to be deserialized).
 */
 public class KafkaValueCaesarCipherEncryptor implements BiFunction<byte[], ClipboardPage, byte[]> {

    @Override
    public byte[] apply(byte[] message, ClipboardPage headers) {
        return transform(message, getShift(headers));
    }

}