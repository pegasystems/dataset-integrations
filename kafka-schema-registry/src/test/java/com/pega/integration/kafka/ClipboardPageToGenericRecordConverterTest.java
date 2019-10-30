package com.pega.integration.kafka;

import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.integration.kafka.converter.ClipboardPageToGenericRecordConverter;
import com.pega.integration.kafka.exception.AvroSerdeException;
import com.pega.integration.kafka.testutils.GenericRecordFactory;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

import static com.pega.integration.kafka.testutils.ClipboardPageComparisonUtils.EPSILON;
import static com.pega.integration.kafka.testutils.ClipboardPageFactory.*;
import static com.pega.integration.kafka.testutils.SchemaFactory.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ClipboardPageToGenericRecordConverterTest {
    private static PublicAPI api;
    private static ClipboardPageToGenericRecordConverter converter;

    @BeforeClass
    public static void before_class() {
        converter = new ClipboardPageToGenericRecordConverter();
        api = new DSMPegaAPI(mock(PegaAPI.class), null);
    }

    @Test(expected = AvroSerdeException.class)
    public void test_against_unsupported_data_type_of_fixed() {
        Schema schema = SchemaBuilder.builder()
                .record("MyRecord")
                .fields()
                .name("myField").type(SchemaBuilder.fixed("myField").size(1)).noDefault()
                .endRecord();

        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("MyClipboardPage", "");
        clipboardPage.putObject("myField", new Object());

        // When
        converter.convertClipboardPageToGenericRecord(clipboardPage, schema);
    }

    @Test(expected = AvroSerdeException.class)
    public void test_against_unsupported_data_type_of_bytes() {
        Schema schema = SchemaBuilder.builder()
                .record("MyRecord")
                .fields()
                .name("myField").type().bytesType().noDefault()
                .endRecord();

        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("MyClipboardPage", "");
        clipboardPage.putObject("myField", new Object());

        // When
        converter.convertClipboardPageToGenericRecord(clipboardPage, schema);
    }

    @Test
    public void test_conversion_of_simple_clipboard_page_to_generic_record() {
        // Given
        ClipboardPage clipboardPage = buildCityClipboardPage(api);
        Schema schema = buildCitySchema();

        // When
        GenericRecord record = converter.convertClipboardPageToGenericRecord(clipboardPage, schema);

        // Then
        assertEquals("Utrecht", record.get("name"));
        assertEquals("NL", record.get("countryCode"));
        assertEquals(99.21, (double) record.get("area"), EPSILON);
        assertEquals(334295, record.get("population"));
    }

    @Test
    public void test_conversion_of_nested_clipboard_page_to_generic_record() {
        // Given
        ClipboardPage countryClipboardPage = buildCountryClipboardPage(api);
        Schema countrySchema = buildCountrySchema();

        // When
        GenericRecord record = converter.convertClipboardPageToGenericRecord(countryClipboardPage, countrySchema);

        // Then
        GenericRecord embeddedRecord = (GenericRecord) record.get("capital");
        assertEquals("The Netherlands", record.get("name"));
        assertEquals("Amsterdam", embeddedRecord.get("name"));
        assertEquals("NL", embeddedRecord.get("countryCode"));
        assertEquals(219.3, (double) embeddedRecord.get("area"), EPSILON);
        assertEquals(821752, embeddedRecord.get("population"));
    }

    @Test
    public void test_conversion_of_big_decimal_value() {
        // Given: ClipboardPage with big decimal field and the corresponding schema
        String className = "ClipboardPageWithBigDecimal";

        ClipboardPage page = api.createPage(className, "");
        BigDecimal expected = new BigDecimal("223355771111.1313");
        page.getProperty("bigDecimalField").setValue(expected);

        Schema pageSchema = SchemaBuilder
                .record(className)
                .fields()
                .name("bigDecimalField").type().stringType().noDefault()
                .endRecord();

        // When
        GenericRecord record = converter.convertClipboardPageToGenericRecord(page, pageSchema);

        // Then
        assertEquals("223355771111.1313", record.get("bigDecimalField"));
    }

    @Test
    public void test_double_values_of_NaN_and_infinity() {
        Schema pageSchema = SchemaBuilder
                .record("NaN_Infinity")
                .fields()
                .name("firstField").type().doubleType().noDefault()
                .name("secondField").type().doubleType().noDefault()
                .name("thirdField").type().doubleType().noDefault()
                .endRecord();

        ClipboardPage page = api.createPage("NaN_Infinity", "");
        page.getProperty("firstField").setValue(Double.NaN);
        page.getProperty("secondField").setValue(Double.NEGATIVE_INFINITY);
        page.getProperty("thirdField").setValue(Double.POSITIVE_INFINITY);

        GenericRecord record = converter.convertClipboardPageToGenericRecord(page, pageSchema);

        assertTrue(Double.isNaN((Double) record.get("firstField")));
        assertTrue(Double.isInfinite((Double) record.get("secondField")));
        assertTrue(Double.isInfinite((Double) record.get("thirdField")));
    }

    @Test
    public void test_conversion_of_clipboard_page_with_embedded_page_to_generic_record() {
        // Given
        ClipboardPage containerPage = buildClipboardPageWithEmbeddedPage(api);
        Schema containerPageSchema = buildSchemaForClipboardPageWithEmbeddedPages();

        // When
        GenericRecord containerRecord = converter.convertClipboardPageToGenericRecord(containerPage, containerPageSchema);

        // Then
        GenericRecord singleEmbeddedRecord = (GenericRecord) containerRecord.get("singleEmbeddedPage");

        assertEquals(53535353535L, containerRecord.get("id"));
        assertEquals("String instance", singleEmbeddedRecord.get("stringField"));
        assertEquals(GenericRecordFactory.MyEnum.A.name(), singleEmbeddedRecord.get("enumField").toString());

        assertEquals(12345678, singleEmbeddedRecord.get("integerField"));
        assertEquals(13579753135L, singleEmbeddedRecord.get("longField"));
        assertEquals(2.45f, (float) singleEmbeddedRecord.get("floatField"), EPSILON);
        assertEquals(4.88, (double) singleEmbeddedRecord.get("doubleField"), EPSILON);
        assertEquals(true, singleEmbeddedRecord.get("booleanField"));

        // assess the fields of the embedded object array
        GenericArray<GenericRecord> embeddedObjectarray = (GenericArray) containerRecord.get("objectArrayField");
        assertEquals("object array, item 1", embeddedObjectarray.get(0).get("stringField"));
        assertEquals(1, embeddedObjectarray.get(0).get("integerField"));
        assertEquals("object array, item 2", embeddedObjectarray.get(1).get("stringField"));
        assertEquals(2, embeddedObjectarray.get(1).get("integerField"));

        // assess the fields of the embedded object map
        Map<String, GenericRecord> embeddedObjectMap = (Map<String, GenericRecord>) containerRecord.get("objectMapField");
        assertEquals("object map, item 1", embeddedObjectMap.get("firstPageInMap").get("stringField"));
        assertEquals("object map, item 2", embeddedObjectMap.get("secondPageInMap").get("stringField"));
    }

    @Test
    public void test_using_sample_customer_kafka_message_schema() throws Exception {
        // Given
        Schema schema = readSchemaFile("sample_customer_kafka_message_schema.json");
        ClipboardPage clipboardPage = buildClipboardPageCompliantWithSampleCustomerSchema(false, api);

        // When
        GenericRecord record = converter.convertClipboardPageToGenericRecord(clipboardPage, schema);

        // Then
        GenericRecord header = (GenericRecord) record.get("header");
        assertEquals("21f6a2bd-7f7c-310f-e053-3619f40adf17", header.get("correlationId"));

        GenericArray processPath = (GenericArray) header.get("processPath");
        GenericRecord firstProcessPathElement = (GenericRecord) processPath.get(0);
        GenericRecord secondProcessPathElement = (GenericRecord) processPath.get(1);

        assertEquals("SAM", firstProcessPathElement.get("source"));
        assertEquals("2018-05-01T10:53:54.000Z", firstProcessPathElement.get("timestamp"));
        assertEquals("JOE", secondProcessPathElement.get("source"));
        assertEquals("2018-02-11T10:53:54.000Z", secondProcessPathElement.get("timestamp"));

        assessEventAndAccountDetails(record);
    }

    @Test
    public void test_using_sample_customer_kafka_message_schema_with_null_collection() throws Exception {
        // Given
        Schema schema = readSchemaFile("sample_customer_kafka_message_schema.json");
        ClipboardPage clipboardPage = buildClipboardPageCompliantWithSampleCustomerSchema(true, api);

        // When
        GenericRecord record = converter.convertClipboardPageToGenericRecord(clipboardPage, schema);

        // Then
        GenericRecord header = (GenericRecord) record.get("header");
        assertEquals("21f6a2bd-7f7c-310f-e053-3619f40adf17", header.get("correlationId"));
        assertEquals(null, header.get("processPath"));

        assessEventAndAccountDetails(record);
    }

    private void assessEventAndAccountDetails(GenericRecord record) {
        GenericRecord event = (GenericRecord) record.get("paymentArrangementStatusChangeEvent");
        assertEquals("S-3624", event.get("paymentArrangementCaseId"));
        assertEquals("COMPLETED", event.get("paymentArrangementStatus"));
        assertEquals("2018-07-01", event.get("arrangementStartDate"));
        assertEquals("2018-08-01", event.get("arrangementEndDate"));
        assertEquals("2018-05-01T10:53:54.000Z", event.get("eventGenerationTime"));

        GenericRecord accountDetails = (GenericRecord) event.get("accountDetails");
        assertEquals("DDA", accountDetails.get("productSystem"));
        assertEquals("56", accountDetails.get("productCode"));
        assertEquals("200014661455", accountDetails.get("accountNumber"));
    }
}