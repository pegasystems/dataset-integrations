package com.pega.integration.kafka.testutils;

import org.apache.avro.Schema;

import java.util.EnumSet;

import static org.apache.avro.Schema.Type.*;

public class PegaPropertyTypesAndModes {

    public static final EnumSet<Schema.Type> SINGLE_VALUE_TYPES = EnumSet.of(ENUM, STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN);

    /**
     * This is used to specify property type of property rule when SINGLE_VALUE mode is selected.
     */
    public enum PropertyType {
        TEXT	 	  {@Override public String toString() { return "Text";}},
        IDENTIFIER	  {@Override public String toString() { return "Identifier"; }},
        PASSWORD	  {@Override public String toString() { return "Password";}},
        INTEGER		  {@Override public String toString() { return "Integer";}},
        DOUBLE	 	  {@Override public String toString() { return "Double";}},
        DECIMAL		  {@Override public String toString() { return "Decimal"; }},
        DATE_TIME	  {@Override public String toString() { return "DateTime";}},
        DATE		  {@Override public String toString() { return "Date";}},
        TIME_OF_DAY	  {@Override public String toString() { return "TimeOfDay";}},
        TRUE_FALSE	  {@Override public String toString() { return "TrueFalse";}},
        TEXT_ENCRYPTED{@Override public String toString() { return "TextEncrypted";}}
    }

    /**
     * This is used to specify property mode of property rule.
     */
    public enum PropertyMode {
        SINGLE_VALUE		{@Override public String toString() { return "String";}},
        VALUE_LIST			{@Override public String toString() { return "StringList"; }},
        VALUE_GROUP			{@Override public String toString() { return "StringGroup";}},
        PAGE				{@Override public String toString() { return "Page";}},
        PAGE_LIST			{@Override public String toString() { return "PageList";}},
        PAGE_GROUP			{@Override public String toString() { return "PageGroup"; }}
    }
}
