package com.pega.integration.kafka.testutils;

import com.pega.decision.dsm.strategy.clipboard.DSMPropertyInfoProvider;

import static com.pega.pegarules.pub.dictionary.ImmutablePropertyInfo.*;

public class PropertyInfoProvider {
    public static DSMPropertyInfoProvider createPropertyInfoForComprehensiveAvroRecord() {
        return MockPropertyInfoProviderBuilder.newBuilder()
                .withEmbeddedPageClass("ComprehensiveRecord", "booleanGroup", "BooleanGroup")
                .withMode("ComprehensiveRecord", "booleanGroup", MODE_PAGE)
                .withType("BooleanGroup", "requiredBoolean", TYPE_TRUEFALSE)
                .withType("BooleanGroup", "requiredBooleanWithDefault", TYPE_TRUEFALSE)
                .withType("BooleanGroup", "optionalBoolean", TYPE_TRUEFALSE)
                .withType("BooleanGroup", "optionalBooleanWithDefault", TYPE_TRUEFALSE)

                .withEmbeddedPageClass("ComprehensiveRecord", "integerGroup", "IntegerGroup")
                .withMode("ComprehensiveRecord", "integerGroup", MODE_PAGE)
                .withType("IntegerGroup", "requiredInt", TYPE_INTEGER)
                .withType("IntegerGroup", "optionalInt", TYPE_INTEGER)
                .withType("IntegerGroup", "optionalIntWithDefault", TYPE_INTEGER)

                .withEmbeddedPageClass("ComprehensiveRecord", "longGroup", "LongGroup")
                .withMode("ComprehensiveRecord", "longGroup", MODE_PAGE)
                .withType("LongGroup", "requiredLong", TYPE_TEXT)
                .withType("LongGroup", "optionalLong", TYPE_TEXT)
                .withType("LongGroup", "optionalLongWithDefault", TYPE_TEXT)

                .withEmbeddedPageClass("ComprehensiveRecord", "floatGroup", "FloatGroup")
                .withMode("ComprehensiveRecord", "floatGroup", MODE_PAGE)
                .withType("FloatGroup", "requiredFloat", TYPE_DOUBLE)
                .withType("FloatGroup", "optionalFloat", TYPE_DOUBLE)
                .withType("FloatGroup", "optionalFloatWithDefault", TYPE_DOUBLE)

                .withEmbeddedPageClass("ComprehensiveRecord", "doubleGroup", "DoubleGroup")
                .withMode("ComprehensiveRecord", "doubleGroup", MODE_PAGE)
                .withType("DoubleGroup", "requiredDouble", TYPE_DOUBLE)
                .withType("DoubleGroup", "optionalDouble", TYPE_DOUBLE)
                .withType("DoubleGroup", "optionalDoubleWithDefault", TYPE_DOUBLE)

                .withEmbeddedPageClass("ComprehensiveRecord", "stringGroup", "StringGroup")
                .withMode("ComprehensiveRecord", "stringGroup", MODE_PAGE)
                .withType("StringGroup", "requiredString", TYPE_TEXT)
                .withType("StringGroup", "optionalString", TYPE_TEXT)
                .withType("StringGroup", "optionalStringWithDefault", TYPE_TEXT)

                .withEmbeddedPageClass("ComprehensiveRecord", "subRecordGroup", "SubRecordGroup")
                .withMode("ComprehensiveRecord", "subRecordGroup", MODE_PAGE)
                .withEmbeddedPageClass("SubRecordGroup", "requiredRecord", "RequiredRecord")
                .withMode("SubRecordGroup", "requiredRecord", MODE_PAGE)
                .withType("RequiredRecord", "subRecordField", TYPE_TRUEFALSE)
                .withEmbeddedPageClass("SubRecordGroup", "optionalRecord", "OptionalRecord")
                .withMode("SubRecordGroup", "optionalRecord", MODE_PAGE)
                .withType("OptionalRecord", "subRecordField", TYPE_TRUEFALSE)
                .withEmbeddedPageClass("SubRecordGroup", "optionalRecordWithDefault", "OptionalRecordWithDefault")
                .withMode("SubRecordGroup", "optionalRecordWithDefault", MODE_PAGE)
                .withType("OptionalRecordWithDefault", "subRecordField", TYPE_TRUEFALSE)

                .withEmbeddedPageClass("ComprehensiveRecord", "enumGroup", "EnumGroup")
                .withMode("ComprehensiveRecord", "enumGroup", MODE_PAGE)
                .withType("EnumGroup", "requiredEnum", TYPE_TEXT)
                .withType("EnumGroup", "optionalEnum", TYPE_TEXT)
                .withType("EnumGroup", "optionalEnumWithDefault", TYPE_TEXT)

                .withEmbeddedPageClass("ComprehensiveRecord", "arrayGroup", "ArrayGroup")
                .withMode("ComprehensiveRecord", "arrayGroup", MODE_PAGE)
                .withMode("ArrayGroup", "requiredArray", MODE_STRING_LIST)
                .withMode("ArrayGroup", "optionalArray", MODE_STRING_LIST)
                .withMode("ArrayGroup", "optionalArrayWithDefault", MODE_STRING_LIST)
                .withMode("ArrayGroup", "arrayOfRecords", MODE_PAGE_LIST)
                .withType("MyArrayElement", "source", TYPE_TEXT)
                .withType("MyArrayElement", "destination", TYPE_INTEGER)
                .withMode("MyArrayElement", "pyPropertyMode", MODE_PAGE)

                .withEmbeddedPageClass("ComprehensiveRecord", "mapGroup", "MapGroup")
                .withMode("ComprehensiveRecord", "mapGroup", MODE_PAGE)
                .withMode("MapGroup", "requiredMap", MODE_STRING_GROUP)
                .withMode("MapGroup", "optionalMap", MODE_STRING_GROUP)
                .withMode("MapGroup", "optionalMapWithDefault", MODE_STRING_GROUP)
                .withMode("MapGroup", "mapOfRecords", MODE_PAGE_GROUP)
                .withType("MyMapElement", "source", TYPE_TEXT)
                .withType("MyMapElement", "destination", TYPE_INTEGER)
                .withMode("MyMapElement", "pyPropertyMode", MODE_PAGE)

                .withEmbeddedPageClass("ComprehensiveRecord", "unionGroup", "UnionGroup")
                .withMode("ComprehensiveRecord", "unionGroup", MODE_PAGE)
                .withType("UnionGroup", "unionBooleanWithDefault", TYPE_TRUEFALSE)
                .withType("UnionGroup", "unionIntWithDefault", TYPE_INTEGER)
                .withType("UnionGroup", "unionLongWithDefault", TYPE_TEXT)
                .withType("UnionGroup", "unionFloatWithDefault", TYPE_DOUBLE)
                .withType("UnionGroup", "unionDoubleWithDefault", TYPE_DOUBLE)
                .withType("UnionGroup", "unionStringWithDefault", TYPE_TEXT)
                .withEmbeddedPageClass("UnionGroup", "unionRecordWithDefault", "UnionRecordWithDefault")
                .withMode("UnionGroup", "unionRecordWithDefault", MODE_PAGE)
                .withType("UnionRecordWithDefault", "nestedRequiredBoolean", TYPE_TRUEFALSE)
                .withType("UnionGroup", "unionEnumWithDefault", TYPE_TEXT)
                .withMode("UnionGroup", "unionArrayWithDefault", MODE_STRING_LIST)
                .withMode("UnionGroup", "unionMapWithDefault", MODE_STRING_GROUP)
                .build();
    }
}
