package com.pega.integration.kafka;

import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.integration.kafka.converter.GenericRecordToClipboardPageConverter;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.pega.integration.kafka.testutils.ClipboardPageComparisonUtils.EPSILON;
import static com.pega.integration.kafka.testutils.GenericRecordFactory.buildEmbeddedGenericRecord;
import static com.pega.integration.kafka.testutils.SchemaFactory.buildCitySchema;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class GenericRecordToClipboardPageConverterTest {
    private static PublicAPI api;
    private static GenericRecordToClipboardPageConverter converter;

    @BeforeClass
    public static void before_class() {
        converter = new GenericRecordToClipboardPageConverter();
        api = new DSMPegaAPI(mock(PegaAPI.class), null);
    }

    @Test
    public void test_conversion_of_generic_record_to_simple_clipboard_page() {
        // Given
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(buildCitySchema());
        GenericRecord record = recordBuilder.set("name", "Amsterdam").set("countryCode", "NL").set("area", 219.3).set("population", 821752).build();

        // When
        ClipboardPage actual = converter.convertGenericRecordToClipboardPage(api, record, "City");

        // Then
        assertEquals("City", actual.getString("pxObjClass"));
        assertEquals("Amsterdam", actual.getString("name"));
        assertEquals("NL", actual.getString("countryCode"));
        assertEquals(219.3, actual.getDouble("area"), EPSILON);
        assertEquals(821752, actual.getInteger("population"));
    }

    @Test
    public void test_conversion_of_generic_record_to_clipboard_page_with_embedded_pages() {
        // Given
        GenericRecord record = buildEmbeddedGenericRecord();

        // When
        ClipboardPage actual = converter.convertGenericRecordToClipboardPage(api, record, "ContainerPage");

        // Then
        assertEquals("ContainerPage", actual.getString("pxObjClass"));
        assertEquals("53535353535", actual.getString("id"));

        ClipboardPage singleEmbeddedPage = actual.getPage("singleEmbeddedPage");
        assertEquals(2.45f, singleEmbeddedPage.getDouble("floatField"), EPSILON);
        assertEquals("13579753135", singleEmbeddedPage.getString("longField"));
        assertEquals(12345678, singleEmbeddedPage.getInteger("integerField"));
        assertEquals("String instance", singleEmbeddedPage.getString("stringField"));
        assertEquals(true, singleEmbeddedPage.getBoolean("booleanField"));
        assertEquals(4.88, singleEmbeddedPage.getDouble("doubleField"), EPSILON);
        assertEquals("B", singleEmbeddedPage.getString("enumField"));
        assertEquals(2.45f, singleEmbeddedPage.getDouble("floatField"), EPSILON);

        ClipboardProperty embeddedObjectMap = actual.getProperty("objectMapField");
        ClipboardPage firstPageInMap = embeddedObjectMap.getPageValue("first");
        assertEquals("object map, item 1", firstPageInMap.getString("stringField"));
        ClipboardPage secondPageInMap = embeddedObjectMap.getPageValue("second");
        assertEquals("object map, item 2", secondPageInMap.getString("stringField"));

        ClipboardProperty embeddedObjectArray = actual.getProperty("objectArrayField");
        assertEquals("object array, item 1", embeddedObjectArray.getPageValue(1).getString("stringField"));
        assertEquals(1, embeddedObjectArray.getPageValue(1).getInteger("integerField"));
        assertEquals("object array, item 2", embeddedObjectArray.getPageValue(2).getString("stringField"));
        assertEquals(2, embeddedObjectArray.getPageValue(2).getInteger("integerField"));
    }
}