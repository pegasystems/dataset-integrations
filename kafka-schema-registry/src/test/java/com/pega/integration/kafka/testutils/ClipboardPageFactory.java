package com.pega.integration.kafka.testutils;

import com.google.common.collect.ImmutableList;
import com.pega.decision.dsm.strategy.clipboard.DSMPegaAPI;
import com.pega.pegarules.priv.PegaAPI;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.runtime.PublicAPI;

import java.util.List;
import java.util.Map;

import static com.pega.integration.kafka.AvroSchemaRegistrySerde.SCHEMA_REGISTRY_CONFIG_CLASS_VALUE;
import static com.pega.integration.kafka.testutils.SchemaFactory.*;

public class ClipboardPageFactory {

    public static ClipboardPage buildSchemaRegistryConfiguration(PublicAPI api, Map config) {
        ClipboardPage schemaRegistryConfig = new DSMPegaAPI((PegaAPI) api).createPage(SCHEMA_REGISTRY_CONFIG_CLASS_VALUE, "");
        schemaRegistryConfig.putAll(config);

        return schemaRegistryConfig;
    }

    public static ClipboardPage buildBankAccountClipboardPage(PublicAPI api) {
        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("BankAccount", "");

        clipboardPage.putString("accountHolder", "Gregory van Avermaet");
        clipboardPage.putString("accountNumber", "201620162016");

        return clipboardPage;
    }

    public static ClipboardPage buildPersonClipboardPage(PublicAPI api) {
        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("Person", "");

        clipboardPage.putString("firstName", "Rigoberto");
        clipboardPage.putString("lastName", "Uran Uran");
        clipboardPage.putObject("age", 32);
        clipboardPage.putString("nickName", "Rigonator");

        return clipboardPage;
    }

    public static ClipboardPage buildCityClipboardPage(PublicAPI api) {
        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("City", "");
        clipboardPage.putString("name", "Utrecht");
        clipboardPage.putString("countryCode", "NL");
        clipboardPage.putObject("area", 99.21);
        clipboardPage.putObject("population", 334295);

        return clipboardPage;
    }

    public static ClipboardPage buildCountryClipboardPage(PublicAPI api) {
        ClipboardPage cityClipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("City", "");
        cityClipboardPage.putString("name", "Amsterdam");
        cityClipboardPage.putString("countryCode", "NL");
        cityClipboardPage.putObject("area", 219.3);
        cityClipboardPage.putObject("population", 821752);

        ClipboardPage countryClipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("Country", "");
        countryClipboardPage.putString("name", "The Netherlands");
        countryClipboardPage.putPage("capital", cityClipboardPage);

        return countryClipboardPage;
    }

    public static ClipboardPage buildClipboardPageWithEmbeddedPage(PublicAPI api) {
        ClipboardPage singleEmbeddedPage = new DSMPegaAPI((PegaAPI) api).createPage(SINGLE_PAGE, "");
        singleEmbeddedPage.putString("stringField", "String instance");
        singleEmbeddedPage.putObject("enumField", GenericRecordFactory.MyEnum.A);
        singleEmbeddedPage.putObject("integerField", 12345678);
        singleEmbeddedPage.putObject("longField", 13579753135L);
        singleEmbeddedPage.putObject("floatField", 2.45f);
        singleEmbeddedPage.putObject("doubleField", 4.88);
        singleEmbeddedPage.putObject("booleanField", true);

        ClipboardPage firstPageInArray = new DSMPegaAPI((PegaAPI) api).createPage(PAGE_IN_ARRAY, "");
        firstPageInArray.putString("stringField", "object array, item 1");
        firstPageInArray.putObject("integerField", 1);

        ClipboardPage secondPageInArray = new DSMPegaAPI((PegaAPI) api).createPage(PAGE_IN_ARRAY, "");
        secondPageInArray.putString("stringField", "object array, item 2");
        secondPageInArray.putObject("integerField", 2);

        ClipboardPage firstPageInMap = new DSMPegaAPI((PegaAPI) api).createPage(PAGE_IN_MAP, "");
        firstPageInMap.putObject("stringField", "object map, item 1");

        ClipboardPage secondPageInMap = new DSMPegaAPI((PegaAPI) api).createPage(PAGE_IN_MAP, "");
        secondPageInMap.putObject("stringField", "object map, item 2");

        ClipboardPage containerPage = new DSMPegaAPI((PegaAPI) api).createPage("ContainerPage", "");
        containerPage.putObject("id", 53535353535L);
        containerPage.putPage("singleEmbeddedPage", singleEmbeddedPage);
        containerPage.putObject("objectArrayField", ImmutableList.of(firstPageInArray, secondPageInArray));

        ClipboardProperty embeddedMap = containerPage.getProperty("objectMapField");
        embeddedMap.getPropertyValue("firstPageInMap").setValue(firstPageInMap);
        embeddedMap.getPropertyValue("secondPageInMap").setValue(secondPageInMap);

        return containerPage;
    }

    public static ClipboardPage buildClipboardPageCompliantWithSampleCustomerSchema(boolean nullCollection, PublicAPI api) {
        List<ClipboardPage> processPathElements = null;

        if (!nullCollection) {
            ClipboardPage firstProcessPathElement = new DSMPegaAPI((PegaAPI) api).createPage("ProcessPathElement", "");
            firstProcessPathElement.putString("source", "SAM");
            firstProcessPathElement.putString("timestamp", "2018-05-01T10:53:54.000Z");

            ClipboardPage secondProcessPathElement = new DSMPegaAPI((PegaAPI) api).createPage("ProcessPathElement", "");
            secondProcessPathElement.putString("source", "JOE");
            secondProcessPathElement.putString("timestamp", "2018-02-11T10:53:54.000Z");

            processPathElements = ImmutableList.of(firstProcessPathElement, secondProcessPathElement);
        }

        ClipboardPage kafkaEventHeader = new DSMPegaAPI((PegaAPI) api).createPage("KafkaEventHeader", "");
        kafkaEventHeader.putString("correlationId", "21f6a2bd-7f7c-310f-e053-3619f40adf17");
        kafkaEventHeader.putObject("processPath", processPathElements);

        ClipboardPage accountDetailsRecord = new DSMPegaAPI((PegaAPI) api).createPage("AccountDetailsRecord", "");
        accountDetailsRecord.putString("productSystem", "DDA");
        accountDetailsRecord.putString("productCode", "56");
        accountDetailsRecord.putString("accountNumber", "200014661455");

        ClipboardPage eventRecord = new DSMPegaAPI((PegaAPI) api).createPage("EventRecord", "");
        eventRecord.putString("paymentArrangementCaseId", "S-3624");
        eventRecord.putString("paymentArrangementStatus", "COMPLETED");
        eventRecord.putString("arrangementStartDate", "2018-07-01");
        eventRecord.putString("arrangementEndDate", "2018-08-01");
        eventRecord.putString("eventGenerationTime", "2018-05-01T10:53:54.000Z");
        eventRecord.putPage("accountDetails", accountDetailsRecord);

        ClipboardPage clipboardPage = new DSMPegaAPI((PegaAPI) api).createPage("PaymentArrangementStatusChangeMessage", "");
        clipboardPage.putPage("header", kafkaEventHeader);
        clipboardPage.putPage("paymentArrangementStatusChangeEvent", eventRecord);

        return clipboardPage;
    }
}
