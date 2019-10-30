package com.pega.integration.kafka.testutils;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import static com.pega.integration.kafka.testutils.SchemaFactory.buildSchemaForClipboardPageWithEmbeddedPages;

public class GenericRecordFactory {
    public static GenericRecord buildEmbeddedGenericRecord() {
        Schema schema = buildSchemaForClipboardPageWithEmbeddedPages();
        Schema arraySchema = schema.getField("objectArrayField").schema();
        Schema mapSchema = schema.getField("objectMapField").schema();
        GenericRecordBuilder firstRecordBuilder = new GenericRecordBuilder(schema.getField("singleEmbeddedPage").schema());
        GenericRecord firstRecord = firstRecordBuilder.set("stringField", "String instance").set("enumField", MyEnum.B).set("integerField", 12345678)
                .set("longField", 13579753135L).set("floatField", 2.45f).set("doubleField", 4.88).set("booleanField", true).set("nullField", null).build();

        Schema arrayElementSchema = arraySchema.getElementType();
        GenericRecordBuilder secondRecordBuilder = new GenericRecordBuilder(arrayElementSchema);
        GenericRecord secondRecord = secondRecordBuilder.set("stringField", "object array, item 1").set("integerField", 1).build();
        GenericRecordBuilder thirdRecordBuilder = new GenericRecordBuilder(arrayElementSchema);
        GenericRecord thirdRecord = thirdRecordBuilder.set("stringField", "object array, item 2").set("integerField", 2).build();

        final GenericArray arrayOfRecords = new GenericData.Array(2, arraySchema);
        arrayOfRecords.add(secondRecord);
        arrayOfRecords.add(thirdRecord);

        Schema mapValueSchema = mapSchema.getValueType();
        GenericRecordBuilder fourthRecordBuilder = new GenericRecordBuilder(mapValueSchema);
        GenericRecord fourthRecord = fourthRecordBuilder.set("stringField", "object map, item 1").build();
        GenericRecordBuilder fifthRecordBuilder = new GenericRecordBuilder(mapValueSchema);
        GenericRecord fifthRecord = fifthRecordBuilder.set("stringField", "object map, item 2").build();

        GenericRecordBuilder containerRecordBuilder = new GenericRecordBuilder(schema);
        return containerRecordBuilder.set("id", 53535353535L).set("singleEmbeddedPage", firstRecord).set("objectArrayField", arrayOfRecords).set("objectMapField",
                ImmutableMap.of("first", fourthRecord, "second", fifthRecord)).build();
    }

    public enum MyEnum {
        A, B, C;
    }
}
