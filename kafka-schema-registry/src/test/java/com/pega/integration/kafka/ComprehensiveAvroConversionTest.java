package com.pega.integration.kafka;

import com.google.common.collect.ImmutableList;
import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.decision.dsm.strategy.clipboard.DSMPropertyInfoProvider;
import com.pega.integration.kafka.converter.ClipboardPageToGenericRecordConverter;
import com.pega.integration.kafka.converter.GenericRecordToClipboardPageConverter;
import com.pega.integration.kafka.testutils.RandomPageGenerator;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static com.pega.integration.kafka.testutils.ClipboardPageComparisonUtils.*;
import static com.pega.integration.kafka.testutils.PropertyInfoProvider.createPropertyInfoForComprehensiveAvroRecord;
import static com.pega.integration.kafka.testutils.SchemaFactory.readSchemaFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ComprehensiveAvroConversionTest {
    private static PublicAPI api;
    private static Schema schema;
    private static RandomPageGenerator generator;
    private static GenericRecordToClipboardPageConverter genericRecordToClipboardPageConverter;
    private static ClipboardPageToGenericRecordConverter clipboardPageToGenericRecordConverter;

    private static ClipboardPage initialPage;
    private static GenericRecord record;
    private static ClipboardPage finalPage;

    @BeforeClass
    public static void beforeClass() throws Exception {
        generator = new RandomPageGenerator();
        DSMPropertyInfoProvider provider = createPropertyInfoForComprehensiveAvroRecord();
        api = new DSMPegaAPI(mock(PegaAPI.class), provider);
        schema = readSchemaFile("comprehensive_avro_schema.json");

        genericRecordToClipboardPageConverter = new GenericRecordToClipboardPageConverter();
        clipboardPageToGenericRecordConverter = new ClipboardPageToGenericRecordConverter();

        initialPage = generator.populate(api, schema, "ComprehensiveRecord");
        record = clipboardPageToGenericRecordConverter.convertClipboardPageToGenericRecord(initialPage, schema);
        finalPage = genericRecordToClipboardPageConverter.convertGenericRecordToClipboardPage(api, record, "ComprehensiveRecord");
    }

    @Test
    public void test_boolean_conversion() {
        GenericRecord booleanGroupRecord = (GenericRecord) record.get("booleanGroup");
        ClipboardPage booleanGroupPage = initialPage.getProperty("booleanGroup").getPageValue();
        assertEquals(booleanGroupPage.getBoolean("requiredBoolean"), booleanGroupRecord.get("requiredBoolean"));
        assertEquals(booleanGroupPage.getBoolean("requiredBooleanWithDefault"), booleanGroupRecord.get("requiredBooleanWithDefault"));

        for (String fieldName : ImmutableList.of("optionalBoolean", "optionalBooleanWithDefault")) {
            Object actual = booleanGroupRecord.get(fieldName) == null ? false : booleanGroupRecord.get(fieldName);
            assertEquals(booleanGroupPage.getBoolean(fieldName), actual);
        }
    }

    @Test
    public void test_integer_conversion() {
        GenericRecord integerGroupRecord = (GenericRecord) record.get("integerGroup");
        ClipboardPage integerGroupPage = initialPage.getProperty("integerGroup").getPageValue();
        assertEquals(integerGroupPage.getInteger("requiredInt"), integerGroupRecord.get("requiredInt"));

        for (String fieldName : ImmutableList.of("optionalInt", "optionalIntWithDefault")) {
            Object actual = integerGroupRecord.get(fieldName) == null ? 0 : integerGroupRecord.get(fieldName);
            assertEquals(integerGroupPage.getInteger(fieldName), actual);
        }
    }

    @Test
    public void test_long_conversion() {
        GenericRecord longGroupRecord = (GenericRecord) record.get("longGroup");
        ClipboardPage longGroupPage = initialPage.getProperty("longGroup").getPageValue();
        assertEquals(longGroupPage.getString("requiredLong"), longGroupRecord.get("requiredLong").toString());

        for (String fieldName : ImmutableList.of("optionalLong", "optionalLongWithDefault")) {
            Object actual = longGroupRecord.get(fieldName) == null ? "" : longGroupRecord.get(fieldName);
            assertEquals(longGroupPage.getString(fieldName), actual.toString());
        }
    }

    @Test
    public void test_float_conversion() {
        GenericRecord floatGroupRecord = (GenericRecord) record.get("floatGroup");
        ClipboardPage floatGroupPage = initialPage.getProperty("floatGroup").getPageValue();

        assertEquals((float) floatGroupPage.getDouble("requiredFloat"), (float) floatGroupRecord.get("requiredFloat"), EPSILON);

        for (String fieldName : ImmutableList.of("optionalFloat", "optionalFloatWithDefault")) {
            Object actual = floatGroupRecord.get(fieldName) == null ? 0f : floatGroupRecord.get(fieldName);
            assertEquals(floatGroupPage.getDouble(fieldName), (float) actual, EPSILON);
        }
    }

    @Test
    public void test_double_conversion() {
        GenericRecord doubleGroupRecord = (GenericRecord) record.get("doubleGroup");
        ClipboardPage doubleGroupPage = initialPage.getProperty("doubleGroup").getPageValue();

        assertEquals(doubleGroupPage.getDouble("requiredDouble"), (double) doubleGroupRecord.get("requiredDouble"), EPSILON);

        for (String fieldName : ImmutableList.of("optionalDouble", "optionalDoubleWithDefault")) {
            Object actual = doubleGroupRecord.get(fieldName) == null ? 0.0 : doubleGroupRecord.get(fieldName);
            assertEquals(doubleGroupPage.getDouble(fieldName), (double) actual, EPSILON);
        }
    }

    @Test
    public void test_string_conversion() {
        GenericRecord stringGroupRecord = (GenericRecord) record.get("stringGroup");
        ClipboardPage stringGroupPage = initialPage.getProperty("stringGroup").getPageValue();

        assertEquals(stringGroupPage.getString("requiredString"), stringGroupRecord.get("requiredString"));

        for (String fieldName : ImmutableList.of("optionalString", "optionalStringWithDefault")) {
            Object actual = stringGroupRecord.get(fieldName) == null ? "" : stringGroupRecord.get(fieldName);
            assertEquals(stringGroupPage.getString(fieldName), actual);
        }
    }

    @Test
    public void test_sub_record_conversion() {
        GenericRecord subRecordGroup = (GenericRecord) record.get("subRecordGroup");
        ClipboardPage subRecordGroupPage = initialPage.getProperty("subRecordGroup").getPageValue();

        boolean expected = subRecordGroupPage.getProperty("requiredRecord").getPageValue().getBoolean("subRecordField");
        Object actual = ((GenericRecord) subRecordGroup.get("requiredRecord")).get("subRecordField");
        assertEquals(expected, actual);

        expected = subRecordGroupPage.getProperty("optionalRecord").getPageValue().getBoolean("subRecordField");
        actual = subRecordGroup.get("optionalRecord") == null ? false : ((GenericRecord) subRecordGroup.get("optionalRecord")).get("subRecordField");
        assertEquals(expected, actual);

        expected = subRecordGroupPage.getProperty("optionalRecordWithDefault").getPageValue().getBoolean("subRecordField");
        actual = subRecordGroup.get("optionalRecordWithDefault") == null ? false : ((GenericRecord) subRecordGroup.get("optionalRecordWithDefault")).get("subRecordField");
        assertEquals(expected, actual);
    }

    @Test
    public void test_enum_conversion() {
        GenericRecord enumGroupRecord = (GenericRecord) record.get("enumGroup");
        ClipboardPage enumGroupPage = initialPage.getProperty("enumGroup").getPageValue();

        assertEquals(enumGroupPage.getString("requiredEnum"), enumGroupRecord.get("requiredEnum"));

        for (String fieldName : ImmutableList.of("optionalEnum", "optionalEnumWithDefault")) {
            Object actual = enumGroupRecord.get(fieldName) == null ? "" : enumGroupRecord.get(fieldName);
            assertEquals(enumGroupPage.getString(fieldName), actual);
        }
    }

    @Test
    public void test_array_conversion() {
        GenericRecord arrayGroupRecord = (GenericRecord) record.get("arrayGroup");
        ClipboardPage arrayGroupPage = initialPage.getProperty("arrayGroup").getPageValue();
        assertTrue(primitiveArraysAreEqual(arrayGroupPage.getProperty("requiredArray"), (GenericData.Array) arrayGroupRecord.get("requiredArray")));
        assertTrue(primitiveArraysAreEqual(arrayGroupPage.getProperty("optionalArray"), (GenericData.Array) arrayGroupRecord.get("optionalArray")));
        assertTrue(primitiveArraysAreEqual(arrayGroupPage.getProperty("optionalArrayWithDefault"), (GenericData.Array) arrayGroupRecord.get("optionalArrayWithDefault")));
        assertTrue(objectArraysAreEqual(arrayGroupPage.getProperty("arrayOfRecords"), (GenericData.Array) arrayGroupRecord.get("arrayOfRecords")));
    }

    @Test
    public void test_map_conversion() {
        GenericRecord mapGroupRecord = (GenericRecord) record.get("mapGroup");
        ClipboardPage mapGroupPage = initialPage.getProperty("mapGroup").getPageValue();
        assertTrue(primitiveMapsAreEqual(mapGroupPage.getProperty("requiredMap"), (Map) mapGroupRecord.get("requiredMap")));
        assertTrue(primitiveMapsAreEqual(mapGroupPage.getProperty("optionalMap"), mapGroupRecord.get("optionalMap") == null ? null : (Map) mapGroupRecord.get("optionalMap")));
        assertTrue(primitiveMapsAreEqual(mapGroupPage.getProperty("optionalMapWithDefault"), mapGroupRecord.get("optionalMapWithDefault") == null ? null : (Map) mapGroupRecord.get("optionalMapWithDefault")));
        assertTrue(objectMapsAreEqual(mapGroupPage.getProperty("mapOfRecords"), (Map) mapGroupRecord.get("mapOfRecords")));
    }

    @Test
    public void test_union_conversion() {
        GenericRecord unionGroupRecord = (GenericRecord) record.get("unionGroup");
        ClipboardPage unionGroupPage = initialPage.getProperty("unionGroup").getPageValue();
        assertEquals(unionGroupPage.getBoolean("unionBooleanWithDefault"), unionGroupRecord.get("unionBooleanWithDefault") == null ? false : unionGroupRecord.get("unionBooleanWithDefault"));
        assertEquals(unionGroupPage.getInteger("unionIntWithDefault"), unionGroupRecord.get("unionIntWithDefault") == null ? 0 : unionGroupRecord.get("unionIntWithDefault"));
        assertEquals(unionGroupPage.getString("unionLongWithDefault"), unionGroupRecord.get("unionLongWithDefault") == null ? "" : unionGroupRecord.get("unionLongWithDefault").toString());
        assertEquals((float) unionGroupPage.getDouble("unionFloatWithDefault"), unionGroupRecord.get("unionFloatWithDefault") == null ? 0f : (float) unionGroupRecord.get("unionFloatWithDefault"), EPSILON);
        assertEquals(unionGroupPage.getDouble("unionDoubleWithDefault"), unionGroupRecord.get("unionDoubleWithDefault") == null ? 0.0 : (double) unionGroupRecord.get("unionDoubleWithDefault"), EPSILON);
        assertEquals(unionGroupPage.getString("unionStringWithDefault"), unionGroupRecord.get("unionStringWithDefault") == null ? "" : unionGroupRecord.get("unionStringWithDefault"));

        boolean expected = unionGroupPage.getProperty("unionRecordWithDefault").getPageValue().getBoolean("nestedRequiredBoolean");
        Object actual = unionGroupRecord.get("unionRecordWithDefault") == null ? false : ((GenericRecord) unionGroupRecord.get("unionRecordWithDefault")).get("nestedRequiredBoolean");
        assertEquals(expected, actual);

        assertEquals(unionGroupPage.getString("unionEnumWithDefault"), unionGroupRecord.get("unionEnumWithDefault") == null ? "" : unionGroupRecord.get("unionEnumWithDefault"));
        assertTrue(primitiveArraysAreEqual(unionGroupPage.getProperty("unionArrayWithDefault"), (GenericData.Array) unionGroupRecord.get("unionArrayWithDefault")));
        assertTrue(primitiveMapsAreEqual(unionGroupPage.getProperty("unionMapWithDefault"), (Map) unionGroupRecord.get("unionMapWithDefault")));
    }

    @Test
    public void test_page_equality_after_serde() {
        assertTrue(clipboardPagesAreEqual(initialPage, finalPage));
    }
}
