package com.pega.integration.kafka.testutils;

import com.google.common.collect.ImmutableList;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.clipboard.ClipboardProperty;
import com.pega.pegarules.pub.dictionary.ImmutablePropertyInfo;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ClipboardPageComparisonUtils {
    public static double EPSILON = Math.pow(10, -6);

    private static final List<String> propertyExclusionList = ImmutableList.of("pxobjclass", "pxsubscript");

    public static boolean primitiveMapsAreEqual(ClipboardProperty mapInPage, Map mapInRecord) {
        if ("".equals(mapInPage.getStringValue()) && mapInRecord == null) {
            return true;
        }

        for (int i = 1; i <= mapInRecord.size(); i++) {
            if (!mapInPage.getProperty(((i - 1) + "")).getStringValue().equals(mapInRecord.get((i - 1) + ""))) {
                return false;
            }
        }

        return true;
    }

    public static boolean objectMapsAreEqual(ClipboardProperty mapInPage, Map<String, GenericData.Record> mapInRecord) {
        if ("".equals(mapInPage.getStringValue()) && mapInRecord == null) {
            return true;
        }

        for (int i = 1; i <= mapInRecord.size(); i++) {
            ClipboardPage propertyValue = mapInPage.getPageValue((i - 1) + "");
            if (!propertyValue.getString("source").equals(mapInRecord.get((i - 1) + "").get("source"))) {
                return false;
            }

            if (propertyValue.getInteger("destination") != (int) (mapInRecord.get((i - 1) + "").get("destination"))) {
                return false;
            }
        }

        return true;
    }

    public static boolean primitiveArraysAreEqual(ClipboardProperty arrayInPage, GenericData.Array<String> arrayInRecord) {
        if ("".equals(arrayInPage.getStringValue()) && arrayInRecord == null) {
            return true;
        }

        for (int i = 1; i <= arrayInPage.size(); i++) {
            ClipboardProperty propertyValue = arrayInPage.getPropertyValue(i);
            if (!primitiveValuesAreEqual(propertyValue.getStringValue(), arrayInRecord.get(i - 1))) {
                return false;
            }
        }

        return true;
    }

    public static boolean objectArraysAreEqual(ClipboardProperty arrayInPage, GenericData.Array<GenericData.Record> arrayInRecord) {
        if ("".equals(arrayInPage.getStringValue()) && arrayInRecord == null) {
            return true;
        }

        for (int i = 1; i <= arrayInPage.size(); i++) {
            ClipboardPage propertyValue = arrayInPage.getPageValue(i);
            if (!propertyValue.getString("source").equals(arrayInRecord.get(i - 1).get("source"))) {
                return false;
            }

            if (propertyValue.getInteger("destination") != (int) (arrayInRecord.get(i - 1).get("destination"))) {
                return false;
            }
        }

        return true;
    }

    public static boolean clipboardPagesAreEqual(ClipboardPage firstPage, ClipboardPage secondPage) {

        for (String propertyName : (Set<String>) firstPage.keySet()) {
            if (propertyExclusionList.contains(propertyName.toLowerCase())) {
                continue;
            }
            ClipboardProperty propertyFromFirstPage = firstPage.getProperty(propertyName);
            ClipboardProperty propertyFromSecondPage = secondPage.getProperty(propertyName);
            if (!clipboardPropertiesAreEqual(propertyFromFirstPage, propertyFromSecondPage)) {
                return false;
            }
        }

        return true;
    }

    private static boolean clipboardPropertiesAreEqual(ClipboardProperty firstProperty, ClipboardProperty secondProperty) {
        char mode = firstProperty.getMode();
        ClipboardPage firstPage, secondPage;
        ClipboardProperty propertyInFirstPage, propertyInSecondPage;
        ClipboardPage pageInFirstPage, pageInSecondPage;

        switch (mode) {
            case ImmutablePropertyInfo.MODE_PAGE:
                if (!clipboardPagesAreEqual(firstProperty.getPageValue(), secondProperty.getPageValue())) {
                    return false;
                }
                break;
            case ImmutablePropertyInfo.MODE_PAGE_GROUP:
                firstPage = firstProperty.getPageValue();
                secondPage = secondProperty.getPageValue();

                for (String propertyName : (Set<String>) firstPage.keySet()) {
                    pageInFirstPage = firstPage.getPage(propertyName);
                    pageInSecondPage = secondPage.getPage(propertyName);

                    if (!clipboardPagesAreEqual(pageInFirstPage, pageInSecondPage)) {
                        return false;
                    }
                }
                break;
            case ImmutablePropertyInfo.MODE_STRING_GROUP:
                firstPage = firstProperty.getPageValue();
                secondPage = secondProperty.getPageValue();

                for (String propertyName : (Set<String>) firstPage.keySet()) {
                    propertyInFirstPage = firstPage.getProperty(propertyName);
                    propertyInSecondPage = secondPage.getProperty(propertyName);

                    if (!clipboardPropertiesAreEqual(propertyInFirstPage, propertyInSecondPage)) {
                        return false;
                    }
                }

                break;
            case ImmutablePropertyInfo.MODE_PAGE_LIST:
                for (int i = 1; i <= firstProperty.size(); i++) {
                    if (!clipboardPagesAreEqual(firstProperty.getPageValue(i), secondProperty.getPageValue(i))) {
                        return false;
                    }
                }
                break;
            case ImmutablePropertyInfo.MODE_STRING_LIST:
                for (int i = 1; i <= firstProperty.size(); i++) {
                    if (!primitiveValuesAreEqual(firstProperty.getObjectValue(i), secondProperty.getObjectValue(i))) {
                        return false;
                    }
                }
                break;
            case ImmutablePropertyInfo.MODE_UNKNOWN:
            case ImmutablePropertyInfo.MODE_STRING:
                Object firstValue = firstProperty.getObjectValue();
                Object secondValue = secondProperty.getObjectValue();
                if (!primitiveValuesAreEqual(firstValue, secondValue)) {
                    return false;
                }
                break;
        }

        return true;
    }

    private static boolean primitiveValuesAreEqual(Object firstValue, Object secondValue) {
        if (firstValue instanceof Float) {
            if (Math.abs((float) firstValue - (float) secondValue) >= EPSILON) {
                return false;
            }
        } else if (firstValue instanceof Double) {
            if (Math.abs((double) firstValue - (double) secondValue) >= EPSILON) {
                return false;
            }
        } else if (!Objects.equals(firstValue, secondValue)) {
            return false;
        }

        return true;
    }
}
