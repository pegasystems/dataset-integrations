package com.pega.integration.kafka.testutils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import static com.pega.integration.kafka.testutils.TestUtils.getFileContentAsString;

public class SchemaFactory {
    public static final String SINGLE_PAGE = "SingleEmbeddedPage";
    public static final String PAGE_IN_ARRAY = "EmbeddedPageInArray";
    public static final String PAGE_IN_MAP = "EmbeddedPageInMap";

    public static Schema buildPersonSchema() {
        return SchemaBuilder
                .record("Person")
                .fields()
                .name("firstName").type().nullable().stringType().noDefault()
                .name("lastName").type().nullable().stringType().noDefault()
                .name("age").type().unionOf().intType().and().stringType().endUnion().noDefault()
                .name("nickName").type().nullable().stringType().stringDefault("nicknameless")
                .endRecord();
    }

    public static Schema buildCitySchema() {
        return SchemaBuilder
                .record("City")
                .fields()
                .name("name").type().nullable().stringType().noDefault()
                .name("countryCode").type().nullable().stringType().stringDefault("unknown")
                .name("area").type().nullable().doubleType().noDefault()
                .name("population").type().nullable().intType().noDefault()
                .endRecord();
    }

    public static Schema buildCountrySchema() {
        Schema citySchema = SchemaBuilder
                .record("City")
                .fields()
                .name("name").type().nullable().stringType().noDefault()
                .name("countryCode").type().nullable().stringType().stringDefault("unknown city")
                .name("area").type().nullable().doubleType().noDefault()
                .name("population").type().nullable().intType().noDefault()
                .endRecord();

        return SchemaBuilder
                .record("Country")
                .fields()
                .name("name").type().nullable().stringType().stringDefault("unknown country")
                .name("capital").type(citySchema).noDefault()
                .endRecord();
    }

    public static Schema buildSchemaForClipboardPageWithEmbeddedPages() {
        Schema singleEmbeddedPageSchema = SchemaBuilder
                .record(SINGLE_PAGE)
                .fields()
                .name("stringField").type().stringType().noDefault()
                .name("enumField").type().enumeration("Category").symbols("A", "B", "C").noDefault()
                .name("integerField").type().intType().noDefault()
                .name("longField").type().longType().noDefault()
                .name("floatField").type().floatType().noDefault()
                .name("doubleField").type().doubleType().noDefault()
                .name("booleanField").type().booleanType().noDefault()
                .name("nullField").type().nullType().nullDefault()
                .endRecord();

        Schema embeddedPageInArraySchema = SchemaBuilder
                .record(PAGE_IN_ARRAY)
                .fields()
                .name("stringField").type().stringType().noDefault()
                .name("integerField").type().intType().noDefault()
                .endRecord();

        Schema embeddedPageInMapSchema = SchemaBuilder
                .record(PAGE_IN_MAP)
                .fields()
                .name("stringField").type().stringType().noDefault()
                .endRecord();

        return SchemaBuilder
                .record("ContainerPage")
                .fields()
                .name("id").type().unionOf().longType().and().stringType().endUnion().noDefault()
                .name("singleEmbeddedPage").type(singleEmbeddedPageSchema).noDefault()
                .name("objectArrayField").type().array().items(embeddedPageInArraySchema).noDefault()
                .name("objectMapField").type().map().values(embeddedPageInMapSchema).noDefault()
                .endRecord();
    }

    public static Schema readSchemaFile(String schemaResourceName) throws Exception {
        String schemaFileContent = getFileContentAsString(schemaResourceName);
        Schema.Parser schemaParser = new Schema.Parser();

        return schemaParser.parse(schemaFileContent);
    }

    public static Schema buildInitialBankAccountSchema() {
        return SchemaBuilder
                .record("BankAccount")
                .fields()
                .name("accountHolder").type().stringType().noDefault()
                .name("accountNumber").type().stringType().noDefault()
                .endRecord();
    }

    public static Schema buildBankAccountSchemaWithANewField() {
        return SchemaBuilder
                .record("BankAccount")
                .fields()
                .name("accountHolder").type().stringType().noDefault()
                .name("accountNumber").type().stringType().noDefault()
                .name("iban").type().stringType().stringDefault("")
                .endRecord();
    }

    public static Schema buildBankAccountSchemaByRemovingAnExistingField() {
        return SchemaBuilder
                .record("BankAccount")
                .fields()
                .name("accountNumber").type().stringType().noDefault()
                .endRecord();
    }

    public static Schema buildIncompatibleBankAccountSchema() {
        return SchemaBuilder
                .record("BankAccount")
                .fields()
                .name("balance").type().doubleType().noDefault()
                .endRecord();
    }
}
