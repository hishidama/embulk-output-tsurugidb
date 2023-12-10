package org.embulk.output.tsurugidb.executor;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum LogLevelOnError {
    NONE, //
    DEBUG_STACKTRACE, //
    DEBUG, //
    INFO, //
    WARN, //
    ERROR, //
    ;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static LogLevelOnError fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown log_level_on_error ''{0}''. Supported value are {1}", //
                    value, Arrays.stream(values()).map(LogLevelOnError::toString).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }
}
