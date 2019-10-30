package com.pega.integration.kafka;

import com.google.common.io.BaseEncoding;
import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.integration.kafka.testutils.SchemaType;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.runtime.PublicAPI;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.pega.integration.kafka.AvroSchemaRegistrySerde.SCHEMA_NAME_FORMAT;
import static com.pega.integration.kafka.testutils.ClipboardPageFactory.buildPersonClipboardPage;
import static com.pega.integration.kafka.testutils.SchemaFactory.buildPersonSchema;
import static com.pega.integration.kafka.testutils.SchemaFactory.readSchemaFile;
import static com.pega.integration.kafka.testutils.TestUtils.getFileContentAsString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AvroSchemaRegistrySerdeTest {
    private static PublicAPI api;
    private static MockSchemaRegistryClient schemaRegistryClient;

    private static final String PERSON_CLASS = "Person";
    private static final String PERSON_TOPIC = "person"; // topic name in Kafka cluster
    private static final String PERSON_SUBJECT = String.format(SCHEMA_NAME_FORMAT, PERSON_TOPIC, SchemaType.value); // schema group identifier used by schema registry

    private static final String SAMPLE_CUSTOMER_CLASS = "PaymentArrangementStatusChangeMessage";
    private static final String SAMPLE_CUSTOMER_TOPIC = "payment_arrangement_status_change_message";
    private static final String SAMPLE_CUSTOMER_SUBJECT = String.format(SCHEMA_NAME_FORMAT, SAMPLE_CUSTOMER_TOPIC, SchemaType.value);

    private static AvroSchemaRegistrySerde objectUnderTestOne;
    private static AvroSchemaRegistrySerde objectUnderTestTwo;

    @BeforeClass
    public static void beforeClass() throws Exception {
        api = new DSMPegaAPI(mock(PegaAPI.class));
        schemaRegistryClient = new MockSchemaRegistryClient();

        Schema personSchema = buildPersonSchema();
        Schema sampleCustomerSchema = readSchemaFile("sample_customer_kafka_message_schema.json");

        schemaRegistryClient.register(PERSON_SUBJECT, personSchema, 1, 1);
        schemaRegistryClient.register(SAMPLE_CUSTOMER_SUBJECT, sampleCustomerSchema, 1, 2);

        objectUnderTestOne = new AvroSchemaRegistrySerde(PERSON_CLASS, PERSON_TOPIC, personSchema, schemaRegistryClient);
        objectUnderTestTwo = new AvroSchemaRegistrySerde(SAMPLE_CUSTOMER_CLASS, SAMPLE_CUSTOMER_TOPIC, sampleCustomerSchema, schemaRegistryClient);
    }

    @Test
    public void test_serialization_of_person_page() {
        // Given
        ClipboardPage clipboardPage = buildPersonClipboardPage(api);

        // When
        byte[] actual = objectUnderTestOne.serialize(api, clipboardPage);

        // Then
        byte[] expected = "\u0000\u0000\u0000\u0000\u0001\u0000\u0012Rigoberto\u0000\u0012Uran Uran\u0000@\u0000\u0012Rigonator".getBytes(UTF_8);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void test_deserialization_of_person_record() {
        // Given
        byte[] messageValueBytes = "\u0000\u0000\u0000\u0000\u0001\u0000\u0012Alejandro\u0000\u0010Valverde\u0000N\u0000\u0012Balaverde".getBytes(UTF_8);

        // When
        ClipboardPage page = objectUnderTestOne.deserialize(api, messageValueBytes);

        // Then
        assertEquals(PERSON_CLASS, page.getString("pxObjClass"));
        assertEquals("Alejandro", page.getString("firstName"));
        assertEquals("Valverde", page.getString("lastName"));
        assertEquals(39, page.getInteger("age"));
        assertEquals("Balaverde", page.getString("nickName"));
    }

    @Test
    public void test_deserialization_of_sample_customer_record() throws Exception {
        // Given
        String sampleCustomerMessageInHex = getFileContentAsString("sample_customer_kafka_message.hex");
        byte[] messageValueBytes = BaseEncoding.base16().decode(sampleCustomerMessageInHex);

        // When
        ClipboardPage containerPage = objectUnderTestTwo.deserialize(api, messageValueBytes);

        // Then
        ClipboardPage header = containerPage.getPage("header");
        assertEquals("21f6a2bd-7f7c-310f-e053-3619f40adf17", header.getString("correlationId"));
        ClipboardProperty processPath = header.getProperty("processPath");

        ClipboardPage firstProcessPathElement = processPath.getPageValue(1);
        assertEquals("SAM", firstProcessPathElement.getString("source"));
        assertEquals("2018-05-01T10:53:54.000Z", firstProcessPathElement.getString("timestamp"));

        ClipboardPage event = containerPage.getPage("paymentArrangementStatusChangeEvent");
        assertEquals("S-3624", event.getString("paymentArrangementCaseId"));
        assertEquals("COMPLETED", event.getString("paymentArrangementStatus"));
        assertEquals("2018-07-01", event.getString("arrangementStartDate"));
        assertEquals("2018-08-01", event.getString("arrangementEndDate"));
        assertEquals("2018-05-01T10:53:54.000Z", event.getString("eventGenerationTime"));

        ClipboardPage accountDetails = event.getPage("accountDetails");
        assertEquals("DDA", accountDetails.getString("productSystem"));
        assertEquals("56", accountDetails.getString("productCode"));
        assertEquals("200014661455", accountDetails.getString("accountNumber"));
    }
}