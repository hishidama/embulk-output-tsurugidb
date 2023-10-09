package org.embulk.output.tsurugidb.insert;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum InsertMethodSub {
    // SQL
    INSERT, INSERT_OR_REPLACE,
    /** @since 0.1.1 */
    INSERT_IF_NOT_EXISTS,
    // KVS
    PUT_OVERWRITE, PUT_IF_ABSENT, PUT_IF_PRESENT;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static InsertMethodSub fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown method_option ''{0}''. Supported options are {1}", //
                    value, Arrays.stream(values()).map(InsertMethodSub::toString).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }

    public static String getUnsupportedMessage(String title, InsertMethodSub option, Collection<InsertMethodSub> validList) {
        return MessageFormat.format("method_option({1}) is not supported in method={0}. Supported options are {2}", //
                title, option, validList.stream().map(InsertMethodSub::toString).collect(Collectors.joining(", ")));
    }
}
