package com.pega.integration.kafka.testutils;

import com.google.common.collect.ImmutableList;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.Schema;
import org.apache.commons.lang.RandomStringUtils;

import java.security.SecureRandom;
import java.util.List;

import static com.pega.integration.kafka.testutils.PegaPropertyTypesAndModes.SINGLE_VALUE_TYPES;
import static org.apache.commons.lang.StringUtils.capitalize;

public class RandomPageGenerator {
    private final SecureRandom random;
    private static List<String> namesOfNullableFields = ImmutableList.of("optionalBoolean", "optionalBooleanWithDefault", "optionalInt", "optionalIntWithDefault",
            "optionalLong", "optionalLongWithDefault", "optionalFloat", "optionalFloatWithDefault", "optionalDouble", "optionalDoubleWithDefault",
            "optionalString", "optionalStringWithDefault", "optionalRecord", "optionalRecordWithDefault", "optionalEnum", "optionalEnumWithDefault",
            "optionalArray", "optionalArrayWithDefault", "arrayOfRecords", "optionalMap", "optionalMapWithDefault",
            "unionBooleanWithDefault", "unionIntWithDefault", "unionLongWithDefault", "unionFloatWithDefault", "unionDoubleWithDefault",
            "unionStringWithDefault", "unionRecordWithDefault", "unionEnumWithDefault", "unionArrayWithDefault", "unionMapWithDefault"
    );

    public RandomPageGenerator() {
        this.random = new SecureRandom();
    }

    public ClipboardPage populate(PublicAPI api, Schema schema, String newClassName) {
        ClipboardPage clipboardPage = api.createPage(newClassName, "");

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            processSchemaField(api, clipboardPage, fieldSchema, fieldName);
        }

        return clipboardPage;
    }

    private void processSchemaField(PublicAPI api, ClipboardPage containerPage, Schema fieldSchema, String fieldName) {
        // Might set null values for the nullable fields.
        if (random.nextBoolean() && namesOfNullableFields.contains(fieldName)) {
            containerPage.putObject(fieldName, null);
            return;
        }

        Schema.Type type = fieldSchema.getType();
        int elementCount;

        switch (type) {
            case RECORD:
                ClipboardPage subPage = populate(api, fieldSchema, capitalize(fieldName));
                containerPage.putPage(fieldName, subPage);
                break;
            case UNION:
                List<Schema> subSchemas = fieldSchema.getTypes();
                for (Schema schema : subSchemas) {
                    if (!Schema.Type.NULL.equals(schema.getType())) {
                        if (SINGLE_VALUE_TYPES.contains(schema.getType())) {
                            containerPage.putObject(fieldName, getRandomData(schema));
                        } else {
                            processSchemaField(api, containerPage, schema, fieldName);
                        }
                        break;
                    }
                }

                break;
            case ARRAY:
                Schema arrayItemSchema = fieldSchema.getElementType();
                elementCount = random.nextInt(10) + 2;
                ClipboardProperty embeddedArray = containerPage.getProperty(fieldName);

                if (SINGLE_VALUE_TYPES.contains(arrayItemSchema.getType())) {
                    while (elementCount-- > 0) {
                        embeddedArray.add(getRandomData(arrayItemSchema));
                    }
                } else {
                    while (elementCount-- > 0) {
                        embeddedArray.add(populate(api, arrayItemSchema, capitalize(arrayItemSchema.getName())));
                    }
                }

                break;
            case MAP:
                Schema mapItemSchema = fieldSchema.getValueType();
                elementCount = random.nextInt(10) + 2;
                ClipboardProperty embeddedMap = containerPage.getProperty(fieldName);

                if (SINGLE_VALUE_TYPES.contains(mapItemSchema.getType())) {
                    while (elementCount-- > 0) {
                        embeddedMap.getPropertyValue(elementCount + "").setValue(getRandomData(mapItemSchema));
                    }
                } else {

                    while (elementCount-- > 0) {
                        embeddedMap.getPropertyValue(elementCount + "").setValue(populate(api, mapItemSchema, capitalize(mapItemSchema.getName())));
                    }
                }

                break;
            case FIXED:
                throw new IllegalArgumentException("Unsupported Avro data type!");
            case BYTES:
                throw new IllegalArgumentException("Unsupported Avro data type!");
            case ENUM:
            case STRING:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                containerPage.putObject(fieldName, getRandomData(fieldSchema));
                break;
            case NULL:
                break;
            default:
                throw new IllegalArgumentException("Couldn't process the given schema field.");
        }
    }

    private Object getRandomData(Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
            case ENUM:
            case STRING:
                return RandomStringUtils.randomAlphabetic(10);
            case INT:
                return random.nextInt();
            case LONG:
                return random.nextLong() + "";
            case FLOAT:
                return random.nextDouble();
            case DOUBLE:
                return random.nextDouble();
            case BOOLEAN:
                return random.nextBoolean();
        }

        return null;
    }
}
