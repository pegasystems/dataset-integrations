package com.pega.integration.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.pega.integration.kafka.converter.ClipboardPageToGenericRecordConverter;
import com.pega.integration.kafka.converter.GenericRecordToClipboardPageConverter;
import com.pega.integration.kafka.exception.AvroSerdeException;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.database.DatabaseException;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.pegarules.pub.util.HashStringMap;
import com.pega.pegarules.pub.util.StringMap;
import com.pega.platform.kafka.serde.PegaSerde;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.pega.integration.kafka.util.SchemaRegistryUtils.parseSchemaContent;
import static com.pega.integration.kafka.util.SchemaRegistryUtils.validateUrl;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.*;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class AvroSchemaRegistrySerde implements PegaSerde {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaRegistrySerde.class);

    public static final String SCHEMA_REGISTRY_CONFIG_CLASS_KEY = "schema.registry.config";
    public static final String SCHEMA_REGISTRY_CONFIG_CLASS_VALUE = "Data-Admin-KafkaSerDe-SchemaRegistry";
    public static final String SCHEMA_NAME_FORMAT = "%s-%s";
    public static final String CLASS_NAME_KEY = "classname";
    public static final String TOPIC_NAME_KEY = "topicname";

    private final ClipboardPageToGenericRecordConverter clipboardPageToGenericRecordConverter;
    private final GenericRecordToClipboardPageConverter genericRecordToClipboardPageConverter;
    private final KafkaAvroSerializer delegateValueSerializer;
    private final KafkaAvroDeserializer delegateValueDeserializer;

    private String className;
    private String topicName;
    private Schema schema;
    private boolean useSchemaEvolution;

    @VisibleForTesting
    public AvroSchemaRegistrySerde() {
        this.clipboardPageToGenericRecordConverter = new ClipboardPageToGenericRecordConverter();
        this.genericRecordToClipboardPageConverter = new GenericRecordToClipboardPageConverter();
        this.delegateValueSerializer = new KafkaAvroSerializer();
        this.delegateValueDeserializer = new KafkaAvroDeserializer();
    }

    /**
     * This constructor is added in order to facilitate writing unit tests.
     * A mock schema registry client is passed to the constructor and Confluent Kafka SerDe is configured with that mock client.
     */
    @VisibleForTesting
    protected AvroSchemaRegistrySerde(String className, String topicName, Schema schema, SchemaRegistryClient schemaRegistryClient) {
        this.clipboardPageToGenericRecordConverter = new ClipboardPageToGenericRecordConverter();
        this.genericRecordToClipboardPageConverter = new GenericRecordToClipboardPageConverter();
        this.delegateValueSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.delegateValueDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        this.className = className;
        this.topicName = topicName;
        this.schema = schema;
    }

    public static PegaSerde create() {
        return new AvroSchemaRegistrySerde();
    }

    @Override
    public void configure(PublicAPI api, Map<String, ?> configuration) {
        validate(configuration);

        className = configuration.get(CLASS_NAME_KEY).toString();
        topicName = configuration.get(TOPIC_NAME_KEY).toString();

        String configName = configuration.get(SCHEMA_REGISTRY_CONFIG_CLASS_KEY).toString();
        ClipboardPage schemaRegistryConfiguration = fetchSchemaRegistryConfiguration(api, configName);
        Map additionalConfiguration = resolveSchemaRegistryConfiguration(schemaRegistryConfiguration);
        additionalConfiguration.putAll(configuration);

        delegateValueSerializer.configure(additionalConfiguration, false);
        delegateValueDeserializer.configure(additionalConfiguration, false);
    }

    private void validate(Map<String, ?> configuration) {
        Preconditions.checkArgument(configuration.containsKey(CLASS_NAME_KEY), "Class name is not configured.");
        Preconditions.checkArgument(StringUtils.isNotBlank(configuration.get(CLASS_NAME_KEY).toString()), "Class name is not configured.");

        Preconditions.checkArgument(configuration.containsKey(TOPIC_NAME_KEY), "Topic name is not configured.");
        Preconditions.checkArgument(StringUtils.isNotBlank(configuration.get(TOPIC_NAME_KEY).toString()), "Topic name is not configured.");

        Preconditions.checkArgument(configuration.containsKey(SCHEMA_REGISTRY_CONFIG_CLASS_KEY), "Schema registry configuration is not provided.");
        Preconditions.checkArgument(StringUtils.isNotBlank(configuration.get(SCHEMA_REGISTRY_CONFIG_CLASS_KEY).toString()), "Schema registry configuration must be provided via '" + SCHEMA_REGISTRY_CONFIG_CLASS_KEY + "' parameter.");
    }

    private ClipboardPage fetchSchemaRegistryConfiguration(PublicAPI api, String configName) {
        StringMap configurationInstanceKeys = new HashStringMap();
        configurationInstanceKeys.put("pxObjClass", SCHEMA_REGISTRY_CONFIG_CLASS_VALUE);
        configurationInstanceKeys.put("ConfigurationName", configName);

        ClipboardPage schemaRegistryConfiguration;
        try {
            schemaRegistryConfiguration = api.getDatabase().open(configurationInstanceKeys, true);
            if (schemaRegistryConfiguration == null) {
                throw new AvroSerdeException("Schema registry configuration named '" + configName + "' couldn't be found.");
            }
        } catch (DatabaseException e) {
            throw new AvroSerdeException("Error occurred while fetching schema registry configuration named '" + configName + "'", e);
        }

        return schemaRegistryConfiguration;
    }

    private Map resolveSchemaRegistryConfiguration(ClipboardPage schemaRegistryConfiguration) {
        Map<String, String> result = new HashMap<>();
        String url = validateUrl(schemaRegistryConfiguration.getString("URL"));
        result.put(SCHEMA_REGISTRY_URL_CONFIG, url);

        String encodedSchemaContent = schemaRegistryConfiguration.getString("SchemaContent");
        if (isNotBlank(encodedSchemaContent)) {
            schema = parseSchemaContent(encodedSchemaContent);
        }

        useSchemaEvolution = schemaRegistryConfiguration.getBoolean("UseSchemaEvolution");

        String authSource = schemaRegistryConfiguration.getString("AuthenticationSource");
        if (StringUtils.isNotBlank(authSource)) {
            result.put(BASIC_AUTH_CREDENTIALS_SOURCE, authSource);
            result.put(USER_INFO_CONFIG, schemaRegistryConfiguration.getString("UserInfo"));
        }

        ClipboardProperty configs = schemaRegistryConfiguration.getProperty("AdditionalConfigurations");
        for (int i = 1; i <= configs.size(); i++) {
            result.put(configs.getStringValue(i, "pyKey"), configs.getStringValue(i, "pyValue"));
        }

        return result;
    }

    @Override
    public byte[] serialize(PublicAPI api, ClipboardPage clipboardPage) {
        GenericRecord record;
        try {
            record = clipboardPageToGenericRecordConverter.convertClipboardPageToGenericRecord(clipboardPage, schema);
            return delegateValueSerializer.serialize(topicName, record);
        } catch (Exception e) {
            LOGGER.error("Conversion of ClipboardPage to GenericRecord has failed.", e);
            throw new AvroSerdeException(e);
        }
    }

    public ClipboardPage deserialize(PublicAPI api, byte[] data) {
        GenericRecord record;
        if (useSchemaEvolution) {
            record = (GenericRecord) delegateValueDeserializer.deserialize(topicName, data, schema);
        } else {
            record = (GenericRecord) delegateValueDeserializer.deserialize(topicName, data);
        }

        return genericRecordToClipboardPageConverter.convertGenericRecordToClipboardPage(api, record, className);
    }

    @VisibleForTesting
    protected Schema getSchema() {
        return schema;
    }

    @VisibleForTesting
    protected void setSchema(Schema schema) {
        this.schema = schema;
    }

    @VisibleForTesting
    protected void useSchemaEvolution(boolean useSchemaEvolution) {
        this.useSchemaEvolution = useSchemaEvolution;
    }
}