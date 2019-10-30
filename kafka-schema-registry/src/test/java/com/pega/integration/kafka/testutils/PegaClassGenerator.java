package com.pega.integration.kafka.testutils;

import com.google.common.collect.ImmutableMap;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.database.DatabaseException;
import com.pega.pegarules.pub.runtime.ParameterPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.Schema;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static com.pega.integration.kafka.util.SchemaRegistryUtils.parseSchemaContent;
import static org.apache.avro.Schema.Type.*;

/**
 * Important: This class is not part of the functionality of the Schema Registry Integration component.
 * It has been added to facilitate the creation of PRPC data classes before performing manual tests regarding Kafka data sets.
 * It can be referred from a PRPC activity, for example. Sample Java code to be fed into activity:
 * <p>
 * String classPrefix = "O4RO6A-Kafkaavro-Int"; // assuming a class named with this prefix exists.
 * String ruleSetName = "Kafkaavro";
 * String ruleSetVersion = "01-01-01";
 * String base64encodedSchema = "ewoidHlwZSI6ICJyZ...";
 * new com.pega.integration.kafka.testutils.PegaClassGenerator(classPrefix, ruleSetName, ruleSetVersion).generate(tools, base64encodedSchema);
 * <p>
 */
public class PegaClassGenerator {
    private static final Map<Schema.Type, PropertyType> avroTypeToPegaTypeMap = ImmutableMap.<Schema.Type, PropertyType>builder()
            .put(ENUM, PropertyType.TEXT)
            .put(STRING, PropertyType.TEXT)
            .put(INT, PropertyType.INTEGER)
            .put(LONG, PropertyType.TEXT)
            .put(FLOAT, PropertyType.DOUBLE)
            .put(DOUBLE, PropertyType.DOUBLE)
            .put(BOOLEAN, PropertyType.TRUE_FALSE)
            .build();

    private static final EnumSet<Schema.Type> singleValueTypes = EnumSet.of(ENUM, STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN);

    private final String classPrefix;
    private final String ruleSetName;
    private final String ruleSetVersion;

    public PegaClassGenerator(String classPrefix, String ruleSetName, String ruleSetVersion) {
        this.classPrefix = classPrefix + "-";
        this.ruleSetName = ruleSetName;
        this.ruleSetVersion = ruleSetVersion;
    }

    public void generate(PublicAPI api, String schemaContent) {
        Schema schema = parseSchemaContent(schemaContent);

        try {
            generate(api, schema, schema.getName());
        } catch (DatabaseException e) {
            throw new IllegalStateException("Unable to generate Pega classes.", e);
        }
    }

    private void generate(PublicAPI api, Schema schema, String classSuffix) throws DatabaseException {
        String newClassName = classPrefix + classSuffix;
        createClass(api, newClassName);

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            processSchemaField(api, fieldSchema, fieldName, newClassName);
        }
    }

    private void processSchemaField(PublicAPI api, Schema fieldSchema, String fieldName, String newClassName) throws DatabaseException {
        Schema.Type type = fieldSchema.getType();
        PropertyMode propertyMode;
        PropertyType propertyType = null;

        switch (type) {
            case RECORD:
                generate(api, fieldSchema, fieldName);
                createProperty(api, newClassName, fieldName, null, PropertyMode.PAGE, classPrefix + fieldName);
                break;
            case UNION:
                List<Schema> subSchemas = fieldSchema.getTypes();
                for (Schema schema : subSchemas) {
                    if (!Schema.Type.NULL.equals(schema.getType())) {
                        processSchemaField(api, schema, fieldName, newClassName);
                    }
                }

                break;
            case ARRAY:
                Schema arrayItemSchema = fieldSchema.getElementType();
                if (singleValueTypes.contains(arrayItemSchema.getType())) {
                    propertyMode = PropertyMode.VALUE_LIST;
                    propertyType = avroTypeToPegaTypeMap.get(arrayItemSchema.getType());
                } else {
                    propertyMode = PropertyMode.PAGE_LIST;
                }

                createClass(api, classPrefix + fieldName);
                createProperty(api, newClassName, fieldName, propertyType, propertyMode, classPrefix + fieldName);
                processSchemaField(api, arrayItemSchema, arrayItemSchema.getName(), classPrefix + fieldName);
                break;
            case MAP:
                Schema mapItemSchema = fieldSchema.getValueType();
                if (singleValueTypes.contains(mapItemSchema.getType())) {
                    propertyMode = PropertyMode.VALUE_GROUP;
                    propertyType = avroTypeToPegaTypeMap.get(mapItemSchema.getType());
                } else {
                    propertyMode = PropertyMode.PAGE_GROUP;
                }

                createClass(api, classPrefix + fieldName);
                createProperty(api, newClassName, fieldName, propertyType, propertyMode, classPrefix + fieldName);
                processSchemaField(api, mapItemSchema, mapItemSchema.getName(), classPrefix + fieldName);
                break;
            case ENUM:
                createProperty(api, newClassName, fieldName, PropertyType.TEXT, PropertyMode.SINGLE_VALUE, null);
                break;
            case FIXED:
                throw new IllegalArgumentException("Unsupported Avro data type!");
            case BYTES:
                throw new IllegalArgumentException("Unsupported Avro data type!");
            case STRING:
                createProperty(api, newClassName, fieldName, PropertyType.TEXT, PropertyMode.SINGLE_VALUE, null);
                break;
            case INT:
                createProperty(api, newClassName, fieldName, PropertyType.INTEGER, PropertyMode.SINGLE_VALUE, null);
                break;
            case LONG:
                createProperty(api, newClassName, fieldName, PropertyType.TEXT, PropertyMode.SINGLE_VALUE, null);
                break;
            case FLOAT:
                createProperty(api, newClassName, fieldName, PropertyType.DOUBLE, PropertyMode.SINGLE_VALUE, null);
                break;
            case DOUBLE:
                createProperty(api, newClassName, fieldName, PropertyType.DOUBLE, PropertyMode.SINGLE_VALUE, null);
                break;
            case BOOLEAN:
                createProperty(api, newClassName, fieldName, PropertyType.TRUE_FALSE, PropertyMode.SINGLE_VALUE, null);
                break;
            case NULL:
                break;
            default:
                throw new IllegalArgumentException("Couldn't process the given schema field.");
        }
    }

    private void createClass(PublicAPI api, String className) throws DatabaseException {
        ClipboardPage clazz = api.createPage("Rule-Obj-Class", "");
        api.applyModel(clazz, new ParameterPage(), "pyDefault");

        clazz.putString("pyClassType", "Concrete");
        clazz.putString("pyClassGroup", className);
        clazz.putString("pyFormType", "Form");
        clazz.putString("pyRuleSet", ruleSetName);
        clazz.putString("pyRuleSetVersion", ruleSetVersion);
        clazz.putString("pyInitialVersion", ruleSetVersion);
        clazz.putString("pyRuleAvailable", "Yes");
        clazz.putString("pyLabel", className);
        clazz.putString("pyDerivesFrom", classPrefix.substring(0, classPrefix.length() - 1));
        clazz.putString("pyRuleName", className);
        clazz.putString("pyUsage", className);
        clazz.putString("pyHasInstances", "false");
        clazz.putString("pyCreateDedicatedTable", "true");
        clazz.putString("pyClassName", className);
        clazz.putString("pyDescription", className);
        clazz.putString("pyClassGroupIndicator", "NOCLASSGROUP");

        api.getDatabase().save(clazz, true, false);
    }

    private void createProperty(PublicAPI api, String propertyClass, String propertyName, PropertyType type, PropertyMode mode, String embeddedClassName) throws DatabaseException {
        ClipboardPage property = api.createPage("Rule-Obj-Property", "");
        api.applyModel(property, new ParameterPage(), "pyDefault");
        property.putString("pyClassName", propertyClass);
        property.putString("pyPropertyName", propertyName);
        property.putString("pyRuleSet", ruleSetName);
        property.putString("pyRuleSetVersion", ruleSetVersion);
        property.putString("pyRuleAvailable", "Yes");
        property.putString("pyLabel", propertyName);
        property.putString("pyUsage", propertyName);
        property.putString("pyDescription", propertyName);
        property.putString("pyColumnInclusion", "Optional");
        property.putString("pyBaseRule", "false");

        if (type != null) {
            property.putString("pyStringType", type.toString());
        }

        if (mode != null) {
            property.putString("pyPropertyMode", mode.toString());
        }

        if (embeddedClassName != null) {
            property.putString("pyPageClass", embeddedClassName);
        }

        api.getDatabase().save(property, true, false);
    }

    /**
     * This is used to specify property type of property rule when SINGLE_VALUE mode is selected.
     */
    private enum PropertyType {
        TEXT	 	  {@Override public String toString() { return "Text";}},
        IDENTIFIER	  {@Override public String toString() { return "Identifier"; }},
        PASSWORD	  {@Override public String toString() { return "Password";}},
        INTEGER		  {@Override public String toString() { return "Integer";}},
        DOUBLE	 	  {@Override public String toString() { return "Double";}},
        DECIMAL		  {@Override public String toString() { return "Decimal"; }},
        DATE_TIME	  {@Override public String toString() { return "DateTime";}},
        DATE		  {@Override public String toString() { return "Date";}},
        TIME_OF_DAY	  {@Override public String toString() { return "TimeOfDay";}},
        TRUE_FALSE	  {@Override public String toString() { return "TrueFalse";}},
        TEXT_ENCRYPTED{@Override public String toString() { return "TextEncrypted";}}
    }

    /**
     * This is used to specify property mode of property rule.
     */
    private enum PropertyMode {
        SINGLE_VALUE		{@Override public String toString() { return "String";}},
        VALUE_LIST			{@Override public String toString() { return "StringList"; }},
        VALUE_GROUP			{@Override public String toString() { return "StringGroup";}},
        PAGE				{@Override public String toString() { return "Page";}},
        PAGE_LIST			{@Override public String toString() { return "PageList";}},
        PAGE_GROUP			{@Override public String toString() { return "PageGroup"; }}
    }
}
