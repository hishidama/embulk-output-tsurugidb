package org.embulk.output.tsurugidb.insert;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum InsertMethod {
    // SQL
    INSERT, INSERT_WAIT, INSERT_BATCH,
    // KVS
    PUT, PUT_WAIT, PUT_BATCH;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static InsertMethod fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown method ''{0}''. Supported modes are {1}", //
                    value, Arrays.stream(values()).map(InsertMethod::toString).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }
}
