package com.pega.integration.kafka.testutils;

import com.pega.decision.dsm.strategy.clipboard.DSMPropertyInfoProvider;
import com.pega.pegarules.data.external.dictionary.PropertyInfoAPI;
import com.pega.pegarules.data.external.dictionary.PropertyInfoAPI.I;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.pega.pegarules.pub.dictionary.ImmutablePropertyInfo.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MockPropertyInfoProviderBuilder {

    private char defaultMode = MODE_STRING;
    private char defaultType = TYPE_TEXT;

    private final DSMPropertyInfoProvider provider;
    private final Map<Entry<String, String>, PropertyInfoAPI.I> infos = new HashMap<>();

    private MockPropertyInfoProviderBuilder() {
        this.provider = mock(DSMPropertyInfoProvider.class);
    }

    public static MockPropertyInfoProviderBuilder newBuilder() {
        return new MockPropertyInfoProviderBuilder();
    }

    public MockPropertyInfoProviderBuilder withMode(String className, String propertyName, char mode) {
        I propertyInfo = propertyInfo(className, propertyName);
        doReturn(mode).when(propertyInfo).getMode();
        doReturn(isList(mode)).when(propertyInfo).isModeList();
        doReturn(isGroup(mode)).when(propertyInfo).isModeGroup();
        doReturn(isPage(mode)).when(propertyInfo).isModePage();
        doReturn(isPageList(mode)).when(propertyInfo).isModePageList();
        doReturn(isPageGroup(mode)).when(propertyInfo).isModePageGroup();
        doReturn(isValueList(mode)).when(propertyInfo).isModeStringList();
        doReturn(isValueGroup(mode)).when(propertyInfo).isModeStringGroup();
        return this;
    }


    public MockPropertyInfoProviderBuilder withType(String className, String propertyName, char type) {
        I propertyInfo = propertyInfo(className, propertyName);
        doReturn(type).when(propertyInfo).getType();
        return this;
    }

    public MockPropertyInfoProviderBuilder withEmbeddedPageClass(String className, String propertyName, String embeddedPageClass) {
        I propertyInfo = propertyInfo(className, propertyName);
        doReturn(embeddedPageClass).when(propertyInfo).getEmbeddedPageClass();
        return this;
    }

    public MockPropertyInfoProviderBuilder withTransient(String className, String propertyName, boolean isTransient) {
        doReturn(isTransient).when(provider).isTransient(className, propertyName);
        return this;
    }

    public DSMPropertyInfoProvider build() {
        return provider;
    }

    private boolean isList(char mode) {
        return MODE_PAGE_LIST == mode || MODE_STRING_LIST == mode;
    }

    private boolean isGroup(char mode) {
        return MODE_PAGE_GROUP == mode || MODE_STRING_GROUP == mode;
    }

    private boolean isPage(char mode) {
        return MODE_PAGE == mode;
    }

    private boolean isPageList(char mode) {
        return MODE_PAGE_LIST == mode;
    }

    private boolean isPageGroup(char mode) {
        return MODE_PAGE_GROUP == mode;
    }

    private boolean isValueList(char mode) {
        return MODE_STRING_LIST == mode;
    }

    private boolean isValueGroup(char mode) {
        return MODE_STRING_GROUP == mode;
    }

    private PropertyInfoAPI.I propertyInfo(final String className, final String propertyName) {
        DefaultMapEntry entry = new DefaultMapEntry(className, propertyName);
        PropertyInfoAPI.I info = infos.get(entry);
        if (info == null) {
            info = mock(PropertyInfoAPI.I.class);
            infos.put(entry, info);
            doReturn(info).when(provider).getPropertyInfo(className == null ? anyString() : className, propertyName == null ? anyString() : propertyName);
            doReturn(defaultMode).when(info).getMode();
            doReturn(defaultType).when(info).getType();
        }
        return info;
    }
}
