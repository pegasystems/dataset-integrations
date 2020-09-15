package com.pega.bigdata.kafka.processor;

import com.pega.pegarules.pub.clipboard.ClipboardPage;

import java.math.BigInteger;

import static java.lang.String.format;

class MessageTransformator {

    static final String ENCRYPTION_KEY = "encryptionKey";

    static int getShift(ClipboardPage headers) {
        String header = headers.getString(ENCRYPTION_KEY);
        if (header.length() != 0) {
            return Integer.parseInt(header);
        } else {
            throw new RuntimeException(format("Header with \"%s\" key wasn't provided.", ENCRYPTION_KEY));
        }
    }

    static byte[] transform(byte[] message, int shift) {
        for (int i = 0; i < message.length; i++) {
            message[i] = (byte) (message[i] + shift);
        }
        return message;
    }

    private static int byteArrayToInt(byte[] byteArray) {
        return new BigInteger(byteArray).intValue();
    }

}
