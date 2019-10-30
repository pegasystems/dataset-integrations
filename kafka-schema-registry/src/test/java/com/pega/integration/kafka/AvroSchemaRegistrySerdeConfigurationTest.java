package com.pega.integration.kafka;

import com.google.common.collect.ImmutableMap;
import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.database.Database;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.pegarules.pub.util.StringMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;

import static com.pega.integration.kafka.AvroSchemaRegistrySerde.*;
import static com.pega.integration.kafka.testutils.ClipboardPageFactory.buildSchemaRegistryConfiguration;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvroSchemaRegistrySerdeConfigurationTest {
    private static PublicAPI api;
    private static Database database;

    private AvroSchemaRegistrySerde objectUnderTest;

    @BeforeClass
    public static void beforeClass() {
        database = mock(Database.class);
        api = new DSMPegaAPI(mock(PegaAPI.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void missing_class_name_should_give_error() {
        objectUnderTest = new AvroSchemaRegistrySerde();
        objectUnderTest.configure(api, Collections.emptyMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void missing_topic_name_should_give_error() {
        objectUnderTest = new AvroSchemaRegistrySerde();
        objectUnderTest.configure(api, ImmutableMap.of(CLASS_NAME_KEY, "YetAnotherClass"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void missing_schema_registry_configuration_should_give_error() {
        objectUnderTest = new AvroSchemaRegistrySerde();
        objectUnderTest.configure(api, ImmutableMap.of(CLASS_NAME_KEY, "YetAnotherClass", TOPIC_NAME_KEY, "yetAnotherTopic"));
    }

    @Test
    public void schema_should_get_configured_given_a_valid_schema_content() throws Exception {
        // Given
        objectUnderTest = new AvroSchemaRegistrySerde();
        String schemaContent = "{\"type\": \"record\", \"name\": \"YetAnother\", \"namespace\": \"no.namespace\", \"doc\": \"This is a test schema.\", \"fields\": [{\"name\": \"firstName\", \"type\": \"string\"}]}";
        String encodedSchemaContent = new String(Base64.getEncoder().encode(schemaContent.getBytes(UTF_8)));
        when(api.getDatabase()).thenReturn(database);
        when(database.open(any(StringMap.class), anyBoolean())).thenReturn(buildSchemaRegistryConfiguration(api, ImmutableMap.of("URL", "http://mySchemaRegistry", "SchemaContent", encodedSchemaContent)));

        // When
        objectUnderTest.configure(api, ImmutableMap.of(CLASS_NAME_KEY, "YetAnotherClass", TOPIC_NAME_KEY, "yetAnotherTopic", SCHEMA_REGISTRY_CONFIG_CLASS_KEY, "MySchemaRegistryConfig"));

        // Then
        Schema expectedSchema = SchemaBuilder
                .record("YetAnother").namespace("no.namespace")
                .doc("This is a test schema.")
                .fields()
                .name("firstName").type().stringType().noDefault()
                .endRecord();
        assertEquals(expectedSchema, objectUnderTest.getSchema());
    }
}