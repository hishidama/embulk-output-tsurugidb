package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.spi.Column;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/Record.java
public interface Record {
    boolean isNull(Column column);

    boolean getBoolean(Column column);

    long getLong(Column column);

    double getDouble(Column column);

    String getString(Column column);

    Instant getTimestamp(Column column);

    Value getJson(Column column);
}
