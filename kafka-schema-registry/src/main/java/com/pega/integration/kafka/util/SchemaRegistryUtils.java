package com.pega.integration.kafka.util;

import com.pega.integration.kafka.exception.AvroSerdeException;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaRegistryUtils {
    // Reference: https://www.owasp.org/index.php/OWASP_Validation_Regex_Repository
    private static final String URL_REGEX =
            "^((((https?|ftps?|gopher|telnet|nntp)://)|(mailto:|news:))" +
                    "(%[0-9A-Fa-f]{2}|[-()_.!~*';/?:@&=+$,A-Za-z0-9])+)" +
                    "([).!';/?:,][[:blank:]])?$";

    private static final Pattern URL_PATTERN = Pattern.compile(URL_REGEX);

    private SchemaRegistryUtils() {
        throw new IllegalStateException("Utility class shouldn't be instantiated.");
    }

    public static String validateUrl(String url) {
        if (StringUtils.isNotBlank(url)) {
            Matcher matcher = URL_PATTERN.matcher(url);
            if (matcher.matches()) {
                return url;
            }
        }

        throw new AvroSerdeException("Schema registry URL is not valid! Ensure that it starts with network protocol type.");
    }

    public static Schema parseSchemaContent(String encodedSchemaContent) {
        try {
            String schemaContent = new String(Base64.getDecoder().decode(encodedSchemaContent));
            Schema.Parser schemaParser = new Schema.Parser();
            return schemaParser.parse(schemaContent);
        } catch (Exception e) {
            throw new AvroSerdeException("Couldn't parse the schema content. Please ensure a valid schema is provided.", e);
        }
    }
}