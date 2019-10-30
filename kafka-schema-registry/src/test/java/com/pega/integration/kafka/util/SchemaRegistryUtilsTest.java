package com.pega.integration.kafka.util;

import com.pega.integration.kafka.exception.AvroSerdeException;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.Base64;

import static com.pega.integration.kafka.util.SchemaRegistryUtils.parseSchemaContent;
import static com.pega.integration.kafka.util.SchemaRegistryUtils.validateUrl;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertNotNull;

public class SchemaRegistryUtilsTest {

    @Test(expected = AvroSerdeException.class)
    public void missing_schema_registry_url_schould_give_error() {
        validateUrl(null);
    }

    @Test(expected = AvroSerdeException.class)
    public void empty_schema_registry_url_schould_give_error() {
        validateUrl("");
    }

    @Test(expected = AvroSerdeException.class)
    public void invalid_schema_registry_url_schould_give_error() {
        validateUrl("localhost:8081");
    }

    @Test
    public void valid_schema_registry_url_succeeds() {
        validateUrl("http://mySchemaRegistry:8081");
    }

    @Test(expected = AvroSerdeException.class)
    public void missing_schema_content_schould_give_error() {
        parseSchemaContent(null);
    }

    @Test(expected = AvroSerdeException.class)
    public void empty_schema_content_schould_give_error() {
        parseSchemaContent("");
    }

    @Test(expected = AvroSerdeException.class)
    public void invalid_schema_content_schould_give_error() {
        parseSchemaContent("{}");
    }

    @Test
    public void valid_schema_content_gets_parsed() {
        String schemaContent = "{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"fullName\", \"type\": \"string\"}]}";
        String encodedSchemaContent = new String(Base64.getEncoder().encode(schemaContent.getBytes(UTF_8)));
        Schema schema = parseSchemaContent(encodedSchemaContent);
        assertNotNull(schema);
    }
}