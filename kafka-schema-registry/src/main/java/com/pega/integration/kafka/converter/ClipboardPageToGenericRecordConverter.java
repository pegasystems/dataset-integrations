package com.pega.integration.kafka.converter;

import com.google.common.collect.ImmutableList;
import com.pega.integration.kafka.exception.AvroSerdeException;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.pega.pegarules.priv.logging.SLF4JLog.FORCE_LOGGING_MARKER;

public class ClipboardPageToGenericRecordConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClipboardPageToGenericRecordConverter.class);

    private static final List<String> propertyExclusionList = ImmutableList.of("pxobjclass", "pxsubscript");
    private static final String UNSUPPORTED_DATA_TYPE_MESSAGE = "Unsupported data type! ";

    public GenericRecord convertClipboardPageToGenericRecord(ClipboardPage page, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        for (String propertyName : (Set<String>) page.keySet()) {
            ClipboardProperty property = page.getProperty(propertyName);
            Schema.Field field = schema.getField(propertyName);

            if (field != null) {
                Schema fieldSchema = field.schema();
                record.put(propertyName, resolveObjectByFieldType(property, fieldSchema));
            }
        }

        return record;
    }

    private Object resolveObjectByFieldType(ClipboardProperty property, Schema schema) {
        Schema.Type type = schema.getType();

        if (property == null) {
            throw new AvroSerdeException("Property is null and null values aren't allowed for this field: " + schema.getName());
        }

        switch (type) {
            case RECORD:
                return convertClipboardPageToGenericRecord(property.getPageValue(), schema);
            case UNION:
                List<Schema> subSchemas = schema.getTypes();
                return handleUnionType(subSchemas, property);
            case ARRAY:
                return resolveClipboardPageArray(schema, property);
            case MAP:
                return resolveClipboardPageMap(schema, property);
            case FIXED:
                throw new AvroSerdeException(UNSUPPORTED_DATA_TYPE_MESSAGE + type);
            case BYTES:
                throw new AvroSerdeException(UNSUPPORTED_DATA_TYPE_MESSAGE + type);
            case LONG:
                return Long.parseLong(property.getStringValue());
            case ENUM:
            case STRING:
                return property.getStringValue();
            case INT:
                return property.toInteger();
            case FLOAT:
                return (float) property.toDouble();
            case DOUBLE:
                return property.toDouble();
            case BOOLEAN:
                if ("true".equalsIgnoreCase(property.getStringValue()) || "false".equalsIgnoreCase(property.getStringValue())) {
                    return property.toBoolean();
                }
            default:
                throw new AvroSerdeException(UNSUPPORTED_DATA_TYPE_MESSAGE + type);
        }
    }

    private Object handleUnionType(List<Schema> fieldSchemas, ClipboardProperty property) {
        if (nullAllowed(fieldSchemas) && property == null) {
                return null;
        }

        for (Schema fieldSchema : fieldSchemas) {
            try {
                return resolveObjectByFieldType(property, fieldSchema);
            } catch (Exception e) {
                LOGGER.warn(FORCE_LOGGING_MARKER, "Property couldn't be matched with the potential schema.", e);
            }
        }

        throw new AvroSerdeException("Property couldn't be evaluated based on the given schema." + property.getName());
    }

    private boolean nullAllowed(List<Schema> fieldSchemas) {
        for (Schema fieldSchema : fieldSchemas) {
            if (Schema.Type.NULL.equals(fieldSchema.getType())) {
                return true;
            }
        }

        return false;
    }

    private Object resolveClipboardPageMap(Schema mapSchema, ClipboardProperty property) {
        final Map<String, Object> mapOfRecords = new HashMap<>();
        ClipboardPage page = property.getPageValue();

        for (String propertyName : (Set<String>) page.keySet()) {
            if (propertyExclusionList.contains(propertyName.toLowerCase())) {
                continue;
            }
            ClipboardProperty entry = page.getProperty(propertyName);
            Schema mapItemSchema = mapSchema.getValueType();
            mapOfRecords.put(propertyName, resolveObjectByFieldType(entry, mapItemSchema));
        }

        return mapOfRecords;
    }

    private Object resolveClipboardPageArray(Schema arraySchema, ClipboardProperty property) {
        Schema arrayItemSchema = arraySchema.getElementType();

        final GenericArray arrayOfRecords = new GenericData.Array(property.size(), arraySchema);

        for (int i = 1; i <= property.size(); i++) {
            ClipboardProperty propertyValue = property.getPropertyValue(i);
            Object record = resolveObjectByFieldType(propertyValue, arrayItemSchema);
            arrayOfRecords.add(record);
        }

        return arrayOfRecords;
    }
}
