package com.pega.integration.kafka.exception;

import org.apache.avro.AvroRuntimeException;

public class AvroSerdeException extends AvroRuntimeException {

    public AvroSerdeException(Throwable cause) {
        super(cause);
    }

    public AvroSerdeException(String message) {
        super(message);
    }

    public AvroSerdeException(String message, Throwable cause) {
        super(message, cause);
    }
}