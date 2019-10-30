package com.pega.integration.kafka.converter;

import com.google.common.primitives.Longs;
import com.pega.integration.kafka.exception.AvroSerdeException;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GenericRecordToClipboardPageConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericRecordToClipboardPageConverter.class);

    public ClipboardPage convertGenericRecordToClipboardPage(PublicAPI api, GenericRecord record, String className) {
        ClipboardPage clipboardPage = api.createPage(className, "");
        populateClipboardPage(clipboardPage, record);

        return clipboardPage;
    }

    private void populateClipboardPage(ClipboardPage clipboardPage, GenericRecord record) {
        Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object fieldValue = record.get(fieldName);
            ClipboardProperty property = clipboardPage.getProperty(fieldName);
            populateProperty(property, fieldValue, field.schema());
        }
    }

    private void populateProperty(ClipboardProperty property, Object value, Schema schema) {
        if (value == null) {
            return;
        }

        Schema.Type type = schema.getType();
        switch (type) {
            case RECORD:
                populateClipboardPage(property.getPageValue(), (GenericRecord) value);
                break;
            case UNION:
                for (Schema subSchema : schema.getTypes()) {
                    if (valueMatchesSchema(subSchema, value)) {
                        populateProperty(property, value, subSchema);
                        break;
                    }
                }
                break;
            case ARRAY:
                GenericArray arrayOfRecords = (GenericArray) value;
                Schema arrayElementSchema = schema.getElementType();

                for (Object record : arrayOfRecords) {
                    ClipboardProperty item = property.getPropertyValue(ClipboardProperty.LIST_APPEND);
                    populateProperty(item, record, arrayElementSchema);
                }
                break;
            case MAP:
                Map<String, Object> mapOfRecords = (Map) value;
                Schema mapValueSchema = schema.getValueType();

                for (Map.Entry<String, Object> entry : mapOfRecords.entrySet()) {
                    ClipboardProperty item = property.getPropertyValue(entry.getKey());
                    populateProperty(item, entry.getValue(), mapValueSchema);
                }
                break;
            case ENUM:
            case LONG:
            case STRING:
                property.setValue(value.toString());
                break;
            case INT:
                property.setValue((int) value);
                break;
            case DOUBLE:
                property.setValue((double) value);
                break;
            case FLOAT:
                property.setValue(((Float) value).doubleValue());
                break;
            case BOOLEAN:
                property.setValue((boolean) value);
                break;
            case NULL:
                break;
            default:
                throw new AvroSerdeException("Unsupported data type: " + type);
        }
    }

    private boolean valueMatchesSchema(Schema schema, Object value) {
        switch (schema.getType()) {
            case RECORD:
                return value instanceof GenericRecord;
            case ENUM:
                return value instanceof GenericData.EnumSymbol || value instanceof String;
            case ARRAY:
                return value instanceof GenericArray;
            case MAP:
                return value instanceof Map;
            case UNION:
                return false;
            case FIXED:
                return false;
            case STRING:
                return value instanceof String || value instanceof Utf8;
            case BYTES:
                return false;
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long || Longs.tryParse(value.toString()) != null;
            case FLOAT:
                return value instanceof Float;
            case DOUBLE:
                return value instanceof Double;
            case BOOLEAN:
                return value instanceof Boolean;
            case NULL:
                return value == null;
        }
        return false;
    }
}