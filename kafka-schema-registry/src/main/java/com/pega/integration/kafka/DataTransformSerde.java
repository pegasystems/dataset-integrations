package com.pega.integration.kafka;

import com.google.common.base.Preconditions;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.ParameterPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.platform.kafka.serde.PegaSerde;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DataTransformSerde implements PegaSerde {

    private static final Logger LOG = LoggerFactory.getLogger(DataTransformSerde.class);
    static final String JSON_DATA_KEY = "jsonData";
    static final String DATA_TRANSFORM_DESERIALIZE_MODE = "DESERIALIZE";
    public static final String DATA_TRANSFORM_NAME_KEY = "data.transform.name";
    public static final String DATA_ClASS_NAME_KEY = "classname";
    private String dataTypeClassName;
    private String dataTransformName;

    public DataTransformSerde() {
    }

    public DataTransformSerde(String dataTypeClassName, String dataTransformName) {
        this.dataTypeClassName = dataTypeClassName;
        this.dataTransformName = dataTransformName;
    }

    @Override
    public void configure(PublicAPI api, Map<String, ?> configuration) {
        validate(configuration);

        dataTypeClassName = configuration.get(DATA_ClASS_NAME_KEY).toString();
        dataTransformName = configuration.get(DATA_TRANSFORM_NAME_KEY).toString();
    }

    private void validate(Map<String, ?> configuration) {
        Preconditions.checkArgument(configuration.containsKey(DATA_TRANSFORM_NAME_KEY), "Data transform name is not configured.");
        Preconditions.checkArgument(StringUtils.isNotBlank(configuration.get(DATA_TRANSFORM_NAME_KEY).toString()), "Data transform name is not configured.");

        Preconditions.checkArgument(configuration.containsKey(DATA_ClASS_NAME_KEY), "Class name isn't configured.");
        Preconditions.checkArgument(StringUtils.isNotBlank(configuration.get(DATA_ClASS_NAME_KEY).toString()), "Class name isn't configured.");
    }

    @Override
    public byte[] serialize(PublicAPI tools, ClipboardPage clipboardPage) {
        return serializeWithUsageOfJsonDataTransform(tools, clipboardPage);
    }

    @Override
    public ClipboardPage deserialize(PublicAPI tools, byte[] data) {
        return deserializeWithUsageOfJsonDataTransform(tools, data);
    }

    private byte[] serializeWithUsageOfJsonDataTransform(PublicAPI tools, ClipboardPage clipboardPage) {
        ParameterPage parameterPage = new ParameterPage();
        try {
            tools.applyModel(clipboardPage, parameterPage, dataTransformName);
        } catch (RuntimeException e) {
            LOG.error("Data transform {} from class {} could not be executed", dataTransformName, dataTypeClassName);
            throw e;
        }
        Object jsonData = parameterPage.get(JSON_DATA_KEY);
        if (jsonData == null) {
            String msg = String.format("Data transform %s from class %s did not return on data", dataTransformName, dataTypeClassName);
            LOG.error(msg);
            throw new NotAJSONDataTransformException(msg);
        }
        return jsonData.toString().getBytes(StandardCharsets.UTF_8);
    }

    private ClipboardPage deserializeWithUsageOfJsonDataTransform(PublicAPI tools, byte[] data) {
        ClipboardPage clipboardPageForDeserializedMessage = tools.createPage(dataTypeClassName, "");
        ParameterPage parameterPage = prepareParameterPageWithDeserializationData(data);

        try {
            tools.applyModel(clipboardPageForDeserializedMessage, parameterPage, dataTransformName);
        } catch (RuntimeException e) {
            LOG.error("Data transform {} from class {} could not be executed", dataTransformName, dataTypeClassName);
            throw e;
        }

        return clipboardPageForDeserializedMessage;
    }

    private ParameterPage prepareParameterPageWithDeserializationData(byte[] dataToDeserialization) {
        ParameterPage parameterPage = new ParameterPage();
        String jsonMessage = new String(dataToDeserialization, StandardCharsets.UTF_8);
        parameterPage.putString(JSON_DATA_KEY, jsonMessage);
        parameterPage.putString("executionMode", DATA_TRANSFORM_DESERIALIZE_MODE);
        return parameterPage;
    }

    static class NotAJSONDataTransformException extends RuntimeException {
        public NotAJSONDataTransformException(String s) {
            super(s);
        }
    }

}