package com.pega.integration.kafka;

import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.dsm.dnode.util.ClipboardPageSerializer;
import com.pega.integration.kafka.testutils.SchemaType;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.pega.integration.kafka.AvroSchemaRegistrySerde.SCHEMA_NAME_FORMAT;
import static com.pega.integration.kafka.testutils.ClipboardPageFactory.buildBankAccountClipboardPage;
import static com.pega.integration.kafka.testutils.SchemaFactory.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class BasicSchemaEvolutionTest {
    private static PublicAPI api;
    private static MockSchemaRegistryClient schemaRegistryClient;

    private static final String BANK_ACCOUNT_CLASS = "BankAccount";
    private static final String BANK_ACCOUNT_TOPIC = "bank_account";
    private static final String BANK_ACCOUNT_SUBJECT = String.format(SCHEMA_NAME_FORMAT, BANK_ACCOUNT_TOPIC, SchemaType.value);

    private static AvroSchemaRegistrySerde objectUnderTest;
    private static ClipboardPage clipboardPage;
    private static byte[] initialKafkaMessage;

    @BeforeClass
    public static void beforeClass() throws Exception {
        api = new DSMPegaAPI(mock(PegaAPI.class));
        schemaRegistryClient = new MockSchemaRegistryClient();

        Schema initialBankAccountSchema = buildInitialBankAccountSchema();
        schemaRegistryClient.register(BANK_ACCOUNT_SUBJECT, initialBankAccountSchema, 1, 1);

        clipboardPage = buildBankAccountClipboardPage(api);
        objectUnderTest = new AvroSchemaRegistrySerde(BANK_ACCOUNT_CLASS, BANK_ACCOUNT_TOPIC, initialBankAccountSchema, schemaRegistryClient);
        initialKafkaMessage = objectUnderTest.serialize(api, clipboardPage);
    }

    @Test(expected = org.apache.kafka.common.errors.SerializationException.class)
    public void test_schema_evolution_with_an_incompatible_schema() {
        objectUnderTest.useSchemaEvolution(true);
        objectUnderTest.setSchema(buildIncompatibleBankAccountSchema());
        objectUnderTest.deserialize(api, initialKafkaMessage);
    }

    @Test
    public void test_schema_evolution_when_a_new_field_is_added() {
        // Before schema evolution is applied
        assessmentsBeforeSchemaEvolution();

        // applying schema evolution
        objectUnderTest.useSchemaEvolution(true);
        objectUnderTest.setSchema(buildBankAccountSchemaWithANewField());

        // After schema evolution is applied
        ClipboardPage evolvedClipboardPage = objectUnderTest.deserialize(api, initialKafkaMessage);
        assertEquals(4, evolvedClipboardPage.size());
        assertEquals("Gregory van Avermaet", evolvedClipboardPage.getString("accountHolder"));
        assertEquals("201620162016", evolvedClipboardPage.getString("accountNumber"));
        assertEquals("", evolvedClipboardPage.getString("iban"));
    }

    @Test
    public void test_schema_evolution_when_an_existing_field_is_removed() {
        // Before schema evolution is applied
        assessmentsBeforeSchemaEvolution();

        // applying schema evolution
        objectUnderTest.useSchemaEvolution(true);
        objectUnderTest.setSchema(buildBankAccountSchemaByRemovingAnExistingField());

        // After schema evolution is applied
        ClipboardPage evolvedClipboardPage = objectUnderTest.deserialize(api, initialKafkaMessage);
        assertEquals(2, evolvedClipboardPage.size());
        assertEquals("201620162016", evolvedClipboardPage.getString("accountNumber"));
    }

    private void assessmentsBeforeSchemaEvolution() {
        objectUnderTest.useSchemaEvolution(false);
        ClipboardPage initialClipboardPage = objectUnderTest.deserialize(api, initialKafkaMessage);
        assertEquals(3, initialClipboardPage.size());
        assertEquals("Gregory van Avermaet", initialClipboardPage.getString("accountHolder"));
        assertEquals("201620162016", initialClipboardPage.getString("accountNumber"));
    }
}